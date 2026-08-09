# Databricks notebook source
# DBTITLE 1,Ingestion Enhanced - TheMealDB Data
# MAGIC %md
# MAGIC # Enhanced Ingestion Pipeline - TheMealDB
# MAGIC
# MAGIC This notebook performs comprehensive data ingestion from TheMealDB API and loads data into Lakebase PostgreSQL tables.
# MAGIC
# MAGIC ## Pipeline Steps:
# MAGIC 1. Extract data from TheMealDB API
# MAGIC 2. Transform and enrich data
# MAGIC 3. Load data into PostgreSQL tables using INSERT statements
# MAGIC
# MAGIC ## Tables:
# MAGIC - `category`: Meal categories
# MAGIC - `ingredients`: Available ingredients
# MAGIC - `country`: Countries/cuisines with regions
# MAGIC - `recipe`: Complete recipe information

# COMMAND ----------

# DBTITLE 1,Imports
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, MapType
from pyspark.sql.functions import udf

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Extract Categories from API

# COMMAND ----------

# DBTITLE 1,Extract and transform category data
# Fetch categories from TheMealDB API
response = requests.get("https://www.themealdb.com/api/json/v1/1/categories.php")
data = response.json()
tbl = data['categories']

# Create DataFrame with category data
df_cat = spark.createDataFrame(tbl)

# Transform: add GUID, rename columns, select relevant fields
df_cat = df_cat.withColumn("id_guid_sk", F.expr("uuid()")) \
       .withColumnRenamed("idCategory", "external_id") \
       .withColumnRenamed("strCategory", "name") \
       .withColumnRenamed("strCategoryDescription", "description") \
       .withColumnRenamed("strCategoryThumb", "imagem") \
       .select("id_guid_sk", "external_id", "name", "description", "imagem")

print(f"Extracted {df_cat.count()} categories")
display(df_cat)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extract Ingredients from API

# COMMAND ----------

# DBTITLE 1,Extract and transform ingredients data
# Fetch ingredients from TheMealDB API
response = requests.get("https://www.themealdb.com/api/json/v1/1/list.php?i=list")
tbl_ing = response.json()
df_ing = spark.createDataFrame(tbl_ing['meals'])

# Transform: add GUID, rename columns, select relevant fields
df_ing = df_ing.withColumn("id_guid_sk", F.expr("uuid()")) \
       .withColumnRenamed("idIngredient", "external_id") \
       .withColumnRenamed("strIngredient", "ingredient_name") \
       .withColumnRenamed("strDescription", "description") \
       .withColumnRenamed("strType", "type_ingredient") \
       .withColumnRenamed("strThumb", "image") \
       .select("id_guid_sk", "external_id", "ingredient_name", "description", "type_ingredient", "image")

print(f"Extracted {df_ing.count()} ingredients")
display(df_ing)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Extract Recipes from API

# COMMAND ----------

# DBTITLE 1,Search for recipes by category
# Get all category names to search for recipes
names = [row['name'] for row in df_cat.select("name").collect()]
dfs = []

# Fetch recipes for each category
for name in names:
    response = requests.get(f"https://www.themealdb.com/api/json/v1/1/filter.php?c={name}")
    tbl_rec = response.json()
    meals = tbl_rec.get('meals', [])
    if meals:
        df = spark.createDataFrame(meals).withColumn("category_name", F.lit(name))
        dfs.append(df)

# Union all recipe DataFrames
df_recipes = dfs[0]
for df in dfs[1:]:
    df_recipes = df_recipes.unionByName(df)

# Transform: add GUID, rename columns
df_recipes = df_recipes.withColumn("id_guid_sk", F.expr("uuid()")) \
       .withColumnRenamed("idMeal", "external_id_meal") \
       .withColumnRenamed("strArea", "area") \
       .withColumnRenamed("strCountry", "country") \
       .withColumnRenamed("strMeal", "meal") \
       .withColumnRenamed("strMealThumb", "image") \
       .select("id_guid_sk", "external_id_meal", "category_name", "area", "country", "meal", "image")

print(f"Extracted {df_recipes.count()} recipes")
display(df_recipes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Extract Full Recipe Details

# COMMAND ----------

# DBTITLE 1,Fetch complete recipe information with ingredients
# Get all recipe IDs
id_receitas = [row['external_id_meal'] for row in df_recipes.select("external_id_meal").collect()]

dfs_lookup = []
for id_receita in id_receitas:
    response = requests.get(f"https://www.themealdb.com/api/json/v1/1/lookup.php?i={id_receita}")
    tbl_lookup = response.json()
    if tbl_lookup.get('meals'):
        schema = StructType([StructField(key, StringType(), True) for key in tbl_lookup['meals'][0].keys()])
        df_lookup = spark.createDataFrame(tbl_lookup['meals'], schema=schema)

        # Build ingredient-measure mapping
        def build_ingredient_measure(row):
            result = {}
            for i in range(1, 21):
                ing = row[f"strIngredient{i}"]
                meas = row[f"strMeasure{i}"]
                if not ing or (not ing.strip()):
                    break
                if not ing and not meas:
                    break
                result[str(i)] = {
                    "ingredient": ing.strip() if ing else "",
                    "measure": meas.strip() if meas else ""
                }
            return result

        ingredient_struct = StructType([
            StructField("ingredient", StringType(), True),
            StructField("measure", StringType(), True)
        ])

        ingredient_measure_udf = udf(build_ingredient_measure, MapType(StringType(), ingredient_struct))
        df_lookup = df_lookup.withColumn("ingredient_measure", ingredient_measure_udf(F.struct([F.col(c) for c in df_lookup.columns])))
        dfs_lookup.append(df_lookup)

# Union all full recipe DataFrames
if dfs_lookup:
    df_full_recipe = dfs_lookup[0]
    for df in dfs_lookup[1:]:
        df_full_recipe = df_full_recipe.unionByName(df)
    print(f"Extracted full details for {df_full_recipe.count()} recipes")
    display(df_full_recipe)

# COMMAND ----------

# DBTITLE 1,Transform fully_recipe data
# Final transformation for fully_recipe
df_full_recipe = df_full_recipe.withColumn("id_guid_sk", F.expr("uuid()")) \
    .withColumnRenamed("idMeal", "external_id") \
    .withColumnRenamed("strMeal", "meal") \
    .withColumnRenamed("strCategory", "category") \
    .withColumnRenamed("strCountry", "country") \
    .withColumnRenamed("strInstructions", "instructions") \
    .withColumnRenamed("strMealThumb", "image") \
    .withColumnRenamed("strYoutube", "video") \
    .withColumnRenamed("strSource", "source_url") \
    .withColumnRenamed("dateModified", "date_modified") \
    .select("id_guid_sk", "external_id", "meal", "category", "country", "instructions", "ingredient_measure", "image", "video", "source_url", "date_modified")

print(f"Fully transformed recipe data ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Extract Countries/Cuisines with Regions

# COMMAND ----------

# DBTITLE 1,Extract countries/cuisines data
# Fetch area/country data from TheMealDB API
response_area = requests.get("https://www.themealdb.com/api/json/v1/1/list.php?a=list")
data_area = response_area.json()
tbl_area = data_area['meals']
df_area = spark.createDataFrame(tbl_area)

# # Transform: add GUID, rename columns
df_area = df_area.withColumn("id_guid_sk", F.expr("uuid()")) \
       .withColumnRenamed("strCountry", "country") \
       .withColumnRenamed("strArea", "cuisine") \
       .select("id_guid_sk", "country", "cuisine")

print(f"Extracted {df_area.count()} countries")
display(df_area)

# COMMAND ----------

# DBTITLE 1,Fetch country acronyms from external API
# Fetch country data with acronyms from GitHub repository
countries = requests.get(
    "https://raw.githubusercontent.com/mledoze/countries/master/countries.json"
).json()

# Build dictionary mapping country name to acronym
country_dict = {
    c["name"]["common"]: c["cca2"]
    for c in countries
}

print(f"Loaded {len(country_dict)} country acronyms")

# COMMAND ----------

# DBTITLE 1,Add acronym column to country data
# Create UDF to map country names to acronyms
acronym_udf = udf(lambda x: country_dict.get(x), StringType())
df_area = df_area.withColumn("acronym", acronym_udf(df_area["country"]))

# Handle special cases for countries not found in the dictionary
df_area = df_area.withColumn(
    "acronym",
    F.when(F.col("country") == "Republic of the Congo", "CG")
     .when(F.col("country") == "Turkey", "TR")
     .otherwise(F.col("acronym"))
)

print("Acronyms added")
display(df_area)

# COMMAND ----------

# DBTITLE 1,Add region information
# Build dictionary mapping acronym to region
region_dict = {
    c["cca2"]: c["subregion"]
    for c in countries
}

# Create UDF to map acronyms to regions
region_udf = udf(lambda x: region_dict.get(x), StringType())
df_area = df_area.withColumn("region", region_udf(df_area["acronym"]))

print("Regions added")
display(df_area)

# COMMAND ----------

# DBTITLE 1,Add country flag images
# Generate flag image URLs using country acronyms
df_area = df_area.withColumn(
    "image", 
    F.concat(
        F.lit("https://flags.restcountries.com/v5/w640/"), 
        F.lower(F.col("acronym")), 
        F.lit(".png")
    )
)

print("Flag images added")
display(df_area)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create Temporary Views for SQL Operations

# COMMAND ----------

# DBTITLE 1,Register DataFrames as temporary views
# Create temporary views for SQL access
df_cat.createOrReplaceTempView("category")
df_ing.createOrReplaceTempView("ingredients")
df_recipes.createOrReplaceTempView("recipe")
df_full_recipe.createOrReplaceTempView("fully_recipe")
df_area.createOrReplaceTempView("country")

print("Temporary views created successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Load Data into PostgreSQL Tables
# MAGIC
# MAGIC ### INSERT INTO Operations
# MAGIC The following cells insert data into PostgreSQL tables defined in the `/sql` folder.

# COMMAND ----------

# DBTITLE 1,Helper function to write to Lakebase PostgreSQL
import sys
import base64
import psycopg2
from psycopg2.extras import execute_batch
from databricks.sdk import WorkspaceClient
from datetime import datetime

_w = WorkspaceClient()

def write_to_lakebase(df, table_name, source_name="themeal_db"):
    """
    Write a Spark DataFrame to a Lakebase PostgreSQL table using psycopg2.
    
    Args:
        df: Spark DataFrame to write
        table_name: Name of the target PostgreSQL table
        source_name: Source identifier for __source column
    """
    # Add metadata columns
    df = df.withColumn("__source", F.lit(source_name)) \
           .withColumn("__ingested_at", F.current_timestamp()) \
           .withColumn("__updated_at", F.current_timestamp())
    
    # Convert to pandas for insertion
    print(f"Converting DataFrame to pandas...")
    pdf = df.toPandas()
    row_count = len(pdf)
    
    # Get connection URL from secret
    url = base64.b64decode(
        _w.secrets.get_secret(scope="database_food_project", key="lakebase-url").value
    ).decode("utf-8")
    
    # Connect and insert
    conn = psycopg2.connect(url)
    try:
        cur = conn.cursor()
        
        # Build INSERT statement dynamically
        columns = list(pdf.columns)
        placeholders = ",".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"
        
        # Convert DataFrame to list of tuples
        data = [tuple(row) for row in pdf.to_numpy()]
        
        # Batch insert
        print(f"Inserting {row_count} rows into '{table_name}'...")
        execute_batch(cur, insert_sql, data, page_size=1000)
        conn.commit()
        
        print(f"✅ Inserted {row_count} rows into table '{table_name}'")
        return row_count
    finally:
        conn.close()

print("Helper function 'write_to_lakebase' ready")

# COMMAND ----------

# DBTITLE 1,INSERT INTO category
# Insert category data into Lakebase PostgreSQL
write_to_lakebase(df_cat, "category")

# COMMAND ----------

# DBTITLE 1,INSERT INTO ingredients
# Insert ingredients data into Lakebase PostgreSQL
write_to_lakebase(df_ing, "ingredients")

# COMMAND ----------

# DBTITLE 1,INSERT INTO country
# Insert country data into Lakebase PostgreSQL
write_to_lakebase(df_area, "country")

# COMMAND ----------

# DBTITLE 1,INSERT INTO recipe
# Prepare recipe data with JOINs before inserting
# Read category table from Lakebase PostgreSQL to get the real primary keys
url = base64.b64decode(
    _w.secrets.get_secret(scope="database_food_project", key="lakebase-url").value
).decode("utf-8")

# Read category from PostgreSQL using psycopg2
conn = psycopg2.connect(url)
try:
    import pandas as pd
    df_category_pd = pd.read_sql("SELECT id_guid_sk, name FROM category", conn)
    df_category_lakebase = spark.createDataFrame(df_category_pd)
    print(f"Loaded {df_category_lakebase.count()} categories from Lakebase")
finally:
    conn.close()

df_recipe_joined = df_recipes.alias("r") \
    .join(df_category_lakebase.alias("c"), F.col("r.category_name") == F.col("c.name")) \
    .join(df_area.alias("co"), F.col("r.country") == F.col("co.country")) \
    .join(df_full_recipe.alias("rf"), F.col("r.external_id_meal") == F.col("rf.external_id")) \
    .select(
        F.col("r.id_guid_sk").alias("id_guid_sk"),
        F.col("r.external_id_meal").alias("external_id_meal"),
        F.col("r.meal").alias("name_meal"),
        F.col("c.id_guid_sk").alias("guid_category"),
        F.col("r.country").alias("country"),
        F.col("co.region").alias("region"),
        F.col("rf.instructions").alias("instructions"),
        F.to_json(F.col("rf.ingredient_measure")).alias("ingredient_measure"),
        F.col("rf.image").alias("image_recipe"),
        F.col("rf.video").alias("video_recipe"),
        F.col("rf.source_url").alias("source_url"),
        F.col("rf.date_modified").alias("date_modified")
    )

print(f"Prepared {df_recipe_joined.count()} recipe records with JOINs")

# Insert recipe data into Lakebase PostgreSQL
write_to_lakebase(df_recipe_joined, "recipe")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Ingestion Complete
# MAGIC
# MAGIC All data has been successfully extracted from TheMealDB API and loaded into PostgreSQL tables:
# MAGIC * ✅ Categories
# MAGIC * ✅ Ingredients
# MAGIC * ✅ Countries with regions
# MAGIC * ✅ Recipes with complete details

# COMMAND ----------

