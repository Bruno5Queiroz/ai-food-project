# Databricks notebook source
# DBTITLE 1,Recipe Update Pipeline with MERGE
# MAGIC %md
# MAGIC # Recipe Update Pipeline - Monthly Incremental Update
# MAGIC
# MAGIC This pipeline performs incremental updates to the recipe table using MERGE operations.
# MAGIC It only processes recipes that have been modified in the last 30 days.
# MAGIC
# MAGIC ## Process Flow:
# MAGIC 1. Extract modified recipes from TheMealDB API (last 30 days)
# MAGIC 2. Transform data and prepare for merge
# MAGIC 3. Execute MERGE statement to update/insert recipes
# MAGIC
# MAGIC ## MERGE Logic:
# MAGIC * **MATCH**: Update existing recipes with new data
# MAGIC * **NO MATCH**: Insert new recipes
# MAGIC
# MAGIC **Scheduled to run:** Monthly

# COMMAND ----------

# DBTITLE 1,Imports and configuration
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, MapType
from pyspark.sql.functions import udf
from datetime import datetime, timedelta

print("Pipeline initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Reference Data
# MAGIC
# MAGIC Load categories and recipes to identify which recipes need updating.

# COMMAND ----------

# DBTITLE 1,Load category data
# Fetch categories from TheMealDB API
response = requests.get("https://www.themealdb.com/api/json/v1/1/categories.php")
data = response.json()
tbl = data['categories']

# Create DataFrame with category data
df_cat = spark.createDataFrame(tbl)

# Transform: add GUID, rename columns
df_cat = df_cat.withColumn("id_guid_sk", F.expr("uuid()")) \
       .withColumnRenamed("idCategory", "external_id") \
       .withColumnRenamed("strCategory", "name") \
       .withColumnRenamed("strCategoryDescription", "description") \
       .withColumnRenamed("strCategoryThumb", "imagem") \
       .select("id_guid_sk", "external_id", "name", "description", "imagem")

print(f"Loaded {df_cat.count()} categories")

# COMMAND ----------

# DBTITLE 1,Load all recipes
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

print(f"Loaded {df_recipes.count()} total recipes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extract Modified Recipes (Last 30 Days)
# MAGIC
# MAGIC Filter recipes based on `dateModified` field to only process recent updates.

# COMMAND ----------

# DBTITLE 1,Fetch modified recipes with date filter
# Calculate date cutoff (last 30 days)
today = datetime.today()
date_cutoff = today - timedelta(days=30)
date_cutoff_str = date_cutoff.strftime("%Y-%m-%d")

print(f"Processing recipes modified since: {date_cutoff_str}")

# Get all recipe IDs
id_receitas = [row['external_id_meal'] for row in df_recipes.select("external_id_meal").collect()]

dfs_lookup = []
modified_count = 0

# Fetch full details for each recipe and filter by date
for id_receita in id_receitas:
    response = requests.get(f"https://www.themealdb.com/api/json/v1/1/lookup.php?i={id_receita}")
    tbl_lookup = response.json()
    if tbl_lookup.get('meals'):
        meal = tbl_lookup['meals'][0]
        date_modified = meal.get('dateModified')
        
        # Only process if date_modified exists and is recent
        if date_modified:
            try:
                date_obj = datetime.strptime(date_modified[:10], "%Y-%m-%d")
                
                # Filter: only recipes modified in last 30 days
                if date_obj >= date_cutoff:
                    modified_count += 1
                    schema = StructType([StructField(key, StringType(), True) for key in meal.keys()])
                    df_lookup = spark.createDataFrame([meal], schema=schema)

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
            except Exception as e:
                # Skip recipes with invalid dates
                pass

print(f"Found {modified_count} recipes modified in last 30 days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Transform Modified Recipe Data

# COMMAND ----------

# DBTITLE 1,Transform fully_recipe data
# Union all modified recipe DataFrames
if dfs_lookup:
    df_to_be_merged = dfs_lookup[0]
    for df in dfs_lookup[1:]:
        df_to_be_merged = df_to_be_merged.unionByName(df)
    
    # Final transformation
    df_to_be_merged = df_to_be_merged.withColumn("id_guid_sk", F.expr("uuid()")) \
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
    
    print(f"Prepared {df_to_be_merged.count()} recipes for merge")
    display(df_to_be_merged)
else:
    print("No modified recipes found in the last 30 days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Load Supporting Dimensions

# COMMAND ----------

# DBTITLE 1,Load country data with regions
# Fetch area/country data
response_area = requests.get("https://www.themealdb.com/api/json/v1/1/list.php?a=list")
data_area = response_area.json()
tbl_area = data_area['meals']
df_area = spark.createDataFrame(tbl_area)

# Fetch country acronyms and regions
countries = requests.get(
    "https://raw.githubusercontent.com/mledoze/countries/master/countries.json"
).json()

country_dict = {c["name"]["common"]: c["cca2"] for c in countries}
region_dict = {c["cca2"]: c["subregion"] for c in countries}

# Transform country data
acronym_udf = udf(lambda x: country_dict.get(x), StringType())
region_udf = udf(lambda x: region_dict.get(x), StringType())

df_area = df_area.withColumn("id_guid_sk", F.expr("uuid()")) \
       .withColumnRenamed("strCountry", "country") \
       .select("id_guid_sk", "country")

df_area = df_area.withColumn("acronym", acronym_udf(df_area["country"]))

# Handle special cases
df_area = df_area.withColumn(
    "acronym",
    F.when(F.col("country") == "Republic of the Congo", "CG")
     .when(F.col("country") == "Turkey", "TR")
     .otherwise(F.col("acronym"))
)

df_area = df_area.withColumn("region", region_udf(df_area["acronym"]))

print(f"Loaded {df_area.count()} countries with regions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create Temporary Views for Merge

# COMMAND ----------

# DBTITLE 1,Register temporary views
# Create temporary views
df_cat.createOrReplaceTempView("category")
df_recipes.createOrReplaceTempView("recipe")
df_area.createOrReplaceTempView("country")

if 'df_to_be_merged' in locals():
    df_to_be_merged.createOrReplaceTempView("fully_recipe")
    print("Temporary views created successfully")
else:
    print("No data to merge - skipping view creation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Execute MERGE INTO Recipe Table
# MAGIC
# MAGIC Perform UPSERT operation:
# MAGIC * **ON MATCH**: Update existing recipes
# MAGIC * **ON NO MATCH**: Insert new recipes

# COMMAND ----------

# DBTITLE 1,MERGE INTO recipe table
# MAGIC %sql
# MAGIC -- MERGE statement to upsert recipe data
# MAGIC -- Matches on external_id_meal and updates or inserts accordingly
# MAGIC
# MAGIC MERGE INTO recipe AS target
# MAGIC USING (
# MAGIC     SELECT 
# MAGIC         r.id_guid_sk,
# MAGIC         r.external_id_meal,
# MAGIC         r.meal as name_meal,
# MAGIC         c.id_guid_sk as guid_category,
# MAGIC         r.country,
# MAGIC         co.region as region,
# MAGIC         rf.instructions,
# MAGIC         rf.ingredient_measure,
# MAGIC         rf.image as image_recipe,
# MAGIC         rf.video as video_recipe,
# MAGIC         rf.source_url,
# MAGIC         rf.date_modified,
# MAGIC         'themeal_db' as __source,
# MAGIC         current_timestamp() as __ingested_at,
# MAGIC         current_timestamp() as __updated_at
# MAGIC     FROM recipe r 
# MAGIC     JOIN category c ON c.name = r.category_name
# MAGIC     JOIN country co ON r.country = co.country
# MAGIC     JOIN fully_recipe rf ON r.external_id_meal = rf.external_id
# MAGIC ) AS source
# MAGIC ON target.external_id_meal = source.external_id_meal
# MAGIC
# MAGIC -- When matched: update all fields except id_guid_sk
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.name_meal = source.name_meal,
# MAGIC     target.guid_category = source.guid_category,
# MAGIC     target.country = source.country,
# MAGIC     target.region = source.region,
# MAGIC     target.instructions = source.instructions,
# MAGIC     target.ingredient_measure = source.ingredient_measure,
# MAGIC     target.image_recipe = source.image_recipe,
# MAGIC     target.video_recipe = source.video_recipe,
# MAGIC     target.source_url = source.source_url,
# MAGIC     target.date_modified = source.date_modified,
# MAGIC     target.__source = source.__source,
# MAGIC     target.__updated_at = current_timestamp()
# MAGIC
# MAGIC -- When not matched: insert new record
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     id_guid_sk,
# MAGIC     external_id_meal,
# MAGIC     name_meal,
# MAGIC     guid_category,
# MAGIC     country,
# MAGIC     region,
# MAGIC     instructions,
# MAGIC     ingredient_measure,
# MAGIC     image_recipe,
# MAGIC     video_recipe,
# MAGIC     source_url,
# MAGIC     date_modified,
# MAGIC     __source,
# MAGIC     __ingested_at,
# MAGIC     __updated_at
# MAGIC ) VALUES (
# MAGIC     source.id_guid_sk,
# MAGIC     source.external_id_meal,
# MAGIC     source.name_meal,
# MAGIC     source.guid_category,
# MAGIC     source.country,
# MAGIC     source.region,
# MAGIC     source.instructions,
# MAGIC     source.ingredient_measure,
# MAGIC     source.image_recipe,
# MAGIC     source.video_recipe,
# MAGIC     source.source_url,
# MAGIC     source.date_modified,
# MAGIC     source.__source,
# MAGIC     source.__ingested_at,
# MAGIC     source.__updated_at
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Pipeline Complete
# MAGIC
# MAGIC Recipe table has been updated with the latest modifications:
# MAGIC * Processed recipes modified in the last 30 days
# MAGIC * Updated existing recipes with new data
# MAGIC * Inserted new recipes not previously in the database
# MAGIC
# MAGIC **Next scheduled run:** In 30 days

# COMMAND ----------

