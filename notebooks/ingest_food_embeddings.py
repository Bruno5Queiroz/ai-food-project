# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Food Recipe Embeddings Pipeline
# MAGIC %md
# MAGIC # Ingest Food Recipe -> Vector Embeddings (Lakebase)
# MAGIC
# MAGIC This notebook computes vector embeddings for food recipes stored in Lakebase PostgreSQL.
# MAGIC
# MAGIC It:
# MAGIC 1. Reads the `recipe` table from Lakebase
# MAGIC 2. Identifies which recipes haven't been embedded yet
# MAGIC 3. Creates embeddings from: `name_meal | country | region | instructions | ingredient_measure`
# MAGIC 4. Stores embeddings in the `recipe_embeddings` table for semantic search
# MAGIC
# MAGIC The embeddings enable:
# MAGIC - Semantic recipe search (find similar recipes)
# MAGIC - Recipe recommendations based on content similarity
# MAGIC - Ingredient-based recipe discovery

# COMMAND ----------

# DBTITLE 1,Install required packages
# MAGIC %pip uninstall -y psycopg2 psycopg2-binary
# MAGIC %pip install -q 'databricks-sdk>=0.118.0' sentence-transformers pandas psycopg2-binary

# COMMAND ----------

# MAGIC %pip uninstall -y psycopg2 psycopg2-binary

# COMMAND ----------

# DBTITLE 1,Restart Python
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Config
# MAGIC %md
# MAGIC ## Config
# MAGIC
# MAGIC Widgets let you override table names and the embedding model without editing the notebook - useful when running this as a scheduled Databricks Job.

# COMMAND ----------

# DBTITLE 1,Widget parameters
dbutils.widgets.text("recipe_table_name", "recipe", "Source table (recipes)")
dbutils.widgets.text("embeddings_table_name", "recipe_embeddings", "Destination table (embeddings)")
dbutils.widgets.text("embedding_model", "sentence-transformers/all-MiniLM-L6-v2", "Embedding model")
dbutils.widgets.text("lakebase_secret_scope", "database_food_project", "Secret scope")
dbutils.widgets.text("lakebase_secret_key", "lakebase-url", "Secret key")

RECIPE_TABLE = dbutils.widgets.get("recipe_table_name")
EMBEDDINGS_TABLE = dbutils.widgets.get("embeddings_table_name")
EMBEDDING_MODEL = dbutils.widgets.get("embedding_model")
LAKEBASE_SECRET_SCOPE = dbutils.widgets.get("lakebase_secret_scope")
LAKEBASE_SECRET_KEY = dbutils.widgets.get("lakebase_secret_key")

print(f"Source table: {RECIPE_TABLE}")
print(f"Embeddings table: {EMBEDDINGS_TABLE}")
print(f"Model: {EMBEDDING_MODEL}")
print(f"Secret: {LAKEBASE_SECRET_SCOPE}/{LAKEBASE_SECRET_KEY}")

# COMMAND ----------

# DBTITLE 1,Resolve Lakebase connection
# MAGIC %md
# MAGIC ## Resolve the Lakebase connection URL
# MAGIC
# MAGIC Fetch the Postgres connection URL from Databricks secrets and parse it into connection parameters for psycopg2.

# COMMAND ----------

# DBTITLE 1,Parse Lakebase connection info
import base64
from urllib.parse import urlparse

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()


def get_lakebase_url() -> str:
    secret = w.secrets.get_secret(scope=LAKEBASE_SECRET_SCOPE, key=LAKEBASE_SECRET_KEY)
    return base64.b64decode(secret.value).decode("utf-8")


lakebase_url = get_lakebase_url()
parsed = urlparse(lakebase_url)

# Extract connection details
db_host = parsed.hostname
db_port = parsed.port or 5432
db_name = parsed.path.lstrip("/")
db_user = parsed.username
db_password = parsed.password

print(f"Host: {db_host}")
print(f"Port: {db_port}")
print(f"Database: {db_name}")
print(f"User: {db_user}")

# COMMAND ----------

# DBTITLE 1,Test connection
import psycopg2

print(f"Testing connection to {db_host}:{db_port}/{db_name}")
print(f"Using authentication as user: {db_user}\n")

try:
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password,
        sslmode='require',
        connect_timeout=10
    )
    print("✅ Connection successful!")
    conn.close()
except Exception as e:
    print(f"❌ Connection failed: {e}")
    raise

# COMMAND ----------

# DBTITLE 1,Load recipes from Lakebase
# MAGIC %md
# MAGIC ## Load recipes from Lakebase
# MAGIC
# MAGIC Fetch all recipes that haven't been embedded yet. We'll identify missing embeddings by finding recipe IDs in the `recipe` table that don't exist in the `recipe_embeddings` table.

# COMMAND ----------

# DBTITLE 1,Load unembedded recipes
import pandas as pd
import psycopg2

conn = psycopg2.connect(
    host=db_host,
    port=db_port,
    dbname=db_name,
    user=db_user,
    password=db_password,
    sslmode='require'
)

try:
    # Find recipes that haven't been embedded yet
    query = f"""
    SELECT 
        r.external_id_meal,
        r.id_guid_sk,
        r.name_meal,
        r.country,
        r.region,
        r.instructions,
        r.ingredient_measure
    FROM {RECIPE_TABLE} r
    LEFT JOIN {EMBEDDINGS_TABLE} e 
        ON r.external_id_meal = e.external_id_meal
    WHERE e.external_id_meal IS NULL
    ORDER BY r.external_id_meal
    """
    
    print(f"Loading unembedded recipes from {RECIPE_TABLE}...")
    df_recipes = pd.read_sql(query, conn)
    conn.close()
    
    print(f"Found {len(df_recipes)} recipes to embed")
    display(df_recipes.head())
except Exception as e:
    conn.close()
    print(f"Error loading recipes: {e}")
    raise

# COMMAND ----------

# DBTITLE 1,Prepare text for embedding
# MAGIC %md
# MAGIC ## Prepare text for embedding
# MAGIC
# MAGIC Combine recipe fields into a single text representation:
# MAGIC `name_meal | country | region | instructions | ingredient_measure`
# MAGIC
# MAGIC This captures the essence of each recipe for semantic search.

# COMMAND ----------

# DBTITLE 1,Build combined text
def build_recipe_text(row):
    """Build a rich text representation of a recipe for embedding."""
    parts = []
    
    # Add meal name
    if pd.notna(row['name_meal']):
        parts.append(f"Recipe: {row['name_meal']}")
    
    # Add country and region
    if pd.notna(row['country']):
        country_text = f"Country: {row['country']}"
        if pd.notna(row['region']):
            country_text += f" ({row['region']})"
        parts.append(country_text)
    
    # Add instructions
    if pd.notna(row['instructions']) and row['instructions'].strip():
        parts.append(f"Instructions: {row['instructions']}")  # 
    # Add ingredients
    if pd.notna(row['ingredient_measure']) and row['ingredient_measure'].strip():
        parts.append(f"Ingredients: {row['ingredient_measure'][:1200]}")  # Limit to 1200 chars
    
    return " | ".join(parts)


df_recipes['combined_text'] = df_recipes.apply(build_recipe_text, axis=1)

print("Sample combined text:")
print(df_recipes['combined_text'].iloc[0][:500] + "...")

# COMMAND ----------

# DBTITLE 1,Compute embeddings
# MAGIC %md
# MAGIC ## Compute embeddings
# MAGIC
# MAGIC Load the sentence-transformers model and compute embeddings for all recipe texts.
# MAGIC
# MAGIC Using `all-MiniLM-L6-v2` which produces 384-dimensional embeddings and is optimized for semantic similarity.

# COMMAND ----------

# DBTITLE 1,Load model and compute embeddings
from sentence_transformers import SentenceTransformer
import numpy as np

print(f"Loading model: {EMBEDDING_MODEL}")
model = SentenceTransformer(EMBEDDING_MODEL)

print(f"Computing embeddings for {len(df_recipes)} recipes...")
embeddings = model.encode(
    df_recipes['combined_text'].tolist(),
    show_progress_bar=True,
    batch_size=32
)

print(f"Embeddings shape: {embeddings.shape}")
print(f"Embedding dimension: {embeddings.shape[1]}")

# Add embeddings to dataframe
df_recipes['embedding'] = [emb.tolist() for emb in embeddings]

# COMMAND ----------

# DBTITLE 1,Insert embeddings into Lakebase
# MAGIC %md
# MAGIC ## Insert embeddings into Lakebase
# MAGIC
# MAGIC Store the computed embeddings in the `recipe_embeddings` table for semantic search.
# MAGIC
# MAGIC Each recipe gets a single embedding (chunk_index=0) representing its full content.

# COMMAND ----------

# DBTITLE 1,Batch insert embeddings
import psycopg2
from psycopg2.extras import execute_values

conn = psycopg2.connect(
    host=db_host,
    port=db_port,
    dbname=db_name,
    user=db_user,
    password=db_password,
    sslmode='require'
)

try:
    cursor = conn.cursor()
    
    # Prepare data for batch insert
    insert_data = []
    for _, row in df_recipes.iterrows():
        insert_data.append((
            row['external_id_meal'],
            row['id_guid_sk'],
            0,  # chunk_index (single chunk per recipe)
            row['combined_text'],
            row['embedding'],
            EMBEDDING_MODEL
        ))
    
    # Batch insert with ON CONFLICT DO NOTHING
    insert_query = f"""
    INSERT INTO {EMBEDDINGS_TABLE} (
        external_id_meal,
        guid_recipe,
        chunk_index,
        chunk_text,
        embedding,
        model_name
    ) VALUES %s
    ON CONFLICT (external_id_meal, chunk_index) DO NOTHING
    """
    
    print(f"Inserting {len(insert_data)} embeddings...")
    execute_values(cursor, insert_query, insert_data)
    
    conn.commit()
    inserted_count = cursor.rowcount
    cursor.close()
    conn.close()
    
    print(f"✅ Successfully inserted {inserted_count} embeddings into {EMBEDDINGS_TABLE}")
except Exception as e:
    conn.rollback()
    conn.close()
    print(f"❌ Error inserting embeddings: {e}")
    raise

# COMMAND ----------

# DBTITLE 1,Verify results
# MAGIC %md
# MAGIC ## Verify results
# MAGIC
# MAGIC Query the embeddings table to confirm the data was inserted correctly.

# COMMAND ----------

# DBTITLE 1,Check embeddings table
conn = psycopg2.connect(
    host=db_host,
    port=db_port,
    dbname=db_name,
    user=db_user,
    password=db_password,
    sslmode='require'
)

try:
    query = f"""
    SELECT 
        COUNT(*) as total_embeddings,
        COUNT(DISTINCT external_id_meal) as unique_recipes,
        model_name
    FROM {EMBEDDINGS_TABLE}
    GROUP BY model_name
    """
    
    df_stats = pd.read_sql(query, conn)
    conn.close()
    
    print("\n" + "="*50)
    print("Embeddings Statistics")
    print("="*50)
    display(df_stats)
    
    print("\n✅ Embedding pipeline completed successfully!")
except Exception as e:
    conn.close()
    print(f"Error checking embeddings: {e}")
    raise

# COMMAND ----------

# DBTITLE 1,Example semantic search
# MAGIC %md
# MAGIC ## Example: Semantic Search
# MAGIC
# MAGIC Demonstrate how to use the embeddings for semantic recipe search.
# MAGIC
# MAGIC Given a query like "spicy chicken dish", find the most similar recipes.

# COMMAND ----------

# DBTITLE 1,Test semantic search
# Example search query
query_text = "spicy chicken dish with rice"

print(f"Searching for: {query_text}\n")

# Compute query embedding
query_embedding = model.encode([query_text])[0].tolist()

# Search for similar recipes using cosine similarity
conn = psycopg2.connect(
    host=db_host,
    port=db_port,
    dbname=db_name,
    user=db_user,
    password=db_password,
    sslmode='require'
)

try:
    search_query = f"""
    SELECT 
        e.external_id_meal,
        r.name_meal,
        r.country,
        r.region,
        e.chunk_text,
        1 - (e.embedding <=> %s::vector) as similarity
    FROM {EMBEDDINGS_TABLE} e
    JOIN {RECIPE_TABLE} r ON e.external_id_meal = r.external_id_meal
    ORDER BY e.embedding <=> %s::vector
    LIMIT 5
    """
    
    df_results = pd.read_sql(
        search_query, 
        conn, 
        params=(str(query_embedding), str(query_embedding))
    )
    conn.close()
    
    print("Top 5 similar recipes:")
    print("="*50)
    for idx, row in df_results.iterrows():
        print(f"\n{idx+1}. {row['name_meal']} ({row['country']})")
        print(f"   Similarity: {row['similarity']:.4f}")
        print(f"   Text preview: {row['chunk_text'][:150]}...")
except Exception as e:
    conn.close()
    print(f"Error during search: {e}")
    raise

# COMMAND ----------

