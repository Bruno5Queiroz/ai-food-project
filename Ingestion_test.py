# Databricks notebook source
# DBTITLE 1,imports
import requests
from pyspark.sql import functions as F


# COMMAND ----------

# DBTITLE 1,category
# tabela categories

response = requests.get("https://www.themealdb.com/api/json/v1/1/categories.php")
data = response.json()
tbl = data['categories']

df_cat = spark.createDataFrame(tbl)

df_cat = df_cat.withColumn("id_guid_sk", F.expr("uuid()")) \
       .withColumnRenamed("idCategory", "external_id") \
       .withColumnRenamed("strCategory", "name") \
       .withColumnRenamed("strCategoryDescription", "description") \
       .withColumnRenamed("strCategoryThumb", "imagem") \
       .select("id_guid_sk", "external_id", "name", "description", "imagem")
display(df_cat)

# COMMAND ----------

# DBTITLE 1,ingredients
response = requests.get("https://www.themealdb.com/api/json/v1/1/list.php?i=list")
tbl_ing = response.json()
df_ing = spark.createDataFrame(tbl_ing['meals'])

df_ing = df_ing.withColumn("id_guid_sk", F.expr("uuid()")) \
       .withColumnRenamed("idIngredient", "external_id") \
       .withColumnRenamed("strIngredient", "ingredient_name") \
       .withColumnRenamed("strDescription", "description") \
        .withColumnRenamed("strType", "type_ingredient") \
        .withColumnRenamed("strThumb", "image") \
       .select("id_guid_sk", "external_id", "ingredient_name", "description", "type_ingredient", "image")


df_ing.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recipe

# COMMAND ----------

# DBTITLE 1,search for recipes
names = [row['name'] for row in df_cat.select("name").collect()]
dfs = []

for name in names:
    response = requests.get(f"https://www.themealdb.com/api/json/v1/1/filter.php?c={name}")
    tbl_rec = response.json()
    meals = tbl_rec.get('meals', [])
    if meals:
        df = spark.createDataFrame(meals).withColumn("category_name", F.lit(name))
        dfs.append(df)

df_recipes = dfs[0]
for df in dfs[1:]:
    df_recipes = df_recipes.unionByName(df)

df_recipes = df_recipes.withColumn("id_guid_sk", F.expr("uuid()")) \
       .withColumnRenamed("idMeal", "external_id_meal") \
       .withColumnRenamed("strArea", "area") \
       .withColumnRenamed("strCountry", "country") \
        .withColumnRenamed("strMeal", "meal") \
        .withColumnRenamed("strMealThumb", "image") \
       .select("id_guid_sk", "external_id_meal", "category_name", "area", "country", "meal", "image")

display(df_recipes)
df_all.groupBy("category_name").count().display()

# COMMAND ----------

# DBTITLE 1,fully recipe
from pyspark.sql.types import StructType, StructField, StringType, MapType
from pyspark.sql.functions import udf

id_receitas = [row['external_id_meal'] for row in df_recipes.select("external_id_meal").collect()]

dfs_lookup = []
for id_receita in id_receitas:
    response = requests.get(f"https://www.themealdb.com/api/json/v1/1/lookup.php?i={id_receita}")
    tbl_lookup = response.json()
    if tbl_lookup.get('meals'):
        schema = StructType([StructField(key, StringType(), True) for key in tbl_lookup['meals'][0].keys()])
        df_lookup = spark.createDataFrame(tbl_lookup['meals'], schema=schema)

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

if dfs_lookup:
    df_full_recipe = dfs_lookup[0]
    for df in dfs_lookup[1:]:
        df_full_recipe = df_full_recipe.unionByName(df)
    display(df_full_recipe)

# COMMAND ----------

# DBTITLE 1,final fully recipe
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

# COMMAND ----------

# DBTITLE 1,pipeline (executa mensalmente)
from pyspark.sql.types import StructType, StructField, StringType, MapType
from pyspark.sql.functions import udf
from datetime import datetime, timedelta

# Calcula a data de corte dinamicamente (últimos 7 dias)
today = datetime.today()
date_cutoff = today - timedelta(days=30)
date_cutoff_str = date_cutoff.strftime("%Y-%m-%d")

id_receitas = [row['external_id_meal'] for row in df_recipes.select("external_id_meal").collect()]

dfs_lookup = []
for id_receita in id_receitas:
    response = requests.get(f"https://www.themealdb.com/api/json/v1/1/lookup.php?i={id_receita}")
    tbl_lookup = response.json()
    if tbl_lookup.get('meals'):
        meal = tbl_lookup['meals'][0]
        date_modified = meal.get('dateModified')
        if date_modified:
            try:
                date_obj = datetime.strptime(date_modified[:10], "%Y-%m-%d")
                if date_obj >= date_cutoff:
                    schema = StructType([StructField(key, StringType(), True) for key in meal.keys()])
                    df_lookup = spark.createDataFrame([meal], schema=schema)

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
            except Exception:
                pass

if dfs_lookup:
    df_to_be_merged = dfs_lookup[0]
    for df in dfs_lookup[1:]:
        df_to_be_merged = df_to_be_merged.unionByName(df)
    
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
    display(df_to_be_merged)

# COMMAND ----------

# MAGIC %md
# MAGIC ## cuisines/country

# COMMAND ----------

response_area = requests.get("https://www.themealdb.com/api/json/v1/1/list.php?a=list")
data_area = response_area.json()
tbl_area = data_area['meals']
tbl_area
df_area = spark.createDataFrame(tbl_area)

# # tabela cuisines
df_area = df_area.withColumn("id_guid_sk", F.expr("uuid()")) \
       .withColumnRenamed("strCountry", "country") \
       .withColumnRenamed("strArea", "cuisine") \
       .select("id_guid_sk", "country", "cuisine")

display(df_area)

# COMMAND ----------

# DBTITLE 1,pega acronimo
countries = requests.get(
    "https://raw.githubusercontent.com/mledoze/countries/master/countries.json"
).json()

country_dict = {
    c["name"]["common"]: c["cca2"]
    for c in countries
}

country_dict

# COMMAND ----------

# DBTITLE 1,cria acronimo na tabela
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

acronym_udf = udf(lambda x: country_dict.get(x), StringType())
df_area = df_area.withColumn("acronym", acronym_udf(df_area["country"]))
display(df_area)

# COMMAND ----------

# DBTITLE 1,valida se tem acronym null
df_area.filter(F.col("acronym").isNull()).display()

# COMMAND ----------

{key: value for key, value in country_dict.items() if key.startswith(('R', 'T', 'C'))}

# COMMAND ----------

df_area = df_area.withColumn(
    "acronym",
    F.when(F.col("country") == "Republic of the Congo", "CG")
     .when(F.col("country") == "Turkey", "TR")
     .otherwise(F.col("acronym"))
)
display(df_area)

# COMMAND ----------

# DBTITLE 1,associa acronimo a imagem
df_area = df_area.withColumn("image", F.concat(F.lit("https://flags.restcountries.com/v5/w640/"), F.lower(F.col("acronym")), F.lit(".png")))
df_area.display()

# COMMAND ----------

# DBTITLE 1,pega regiao
import requests 

countries = requests.get(
    "https://raw.githubusercontent.com/mledoze/countries/master/countries.json"
).json()

region_dict = {
    c["cca2"]: c["subregion"]
    for c in countries
}

region_dict

# COMMAND ----------

# DBTITLE 1,cria regiao na tabela
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

region_udf = udf(lambda x: region_dict.get(x), StringType())
df_area = df_area.withColumn("region", region_udf(df_area["acronym"]))
display(df_area)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Version of Tables for SQL

# COMMAND ----------

# DBTITLE 1,views
df_cat.createOrReplaceTempView("category")
df_ing.createOrReplaceTempView("ingredients")
df_recipes.createOrReplaceTempView("recipe")
df_full_recipe.createOrReplaceTempView("fully_recipe")
df_area.createOrReplaceTempView("country")



# COMMAND ----------

# DBTITLE 1,category
# MAGIC %sql
# MAGIC select *, 'themeal_db' as __source, current_timestamp() as __ingested_at, current_timestamp() as __updated_at
# MAGIC from category

# COMMAND ----------

# DBTITLE 1,ingredients
# MAGIC %sql
# MAGIC select *, 'themeal_db' as __source, current_timestamp() as __ingested_at, current_timestamp() as __updated_at
# MAGIC from ingredients

# COMMAND ----------

# DBTITLE 1,country
# MAGIC %sql
# MAGIC select *, 'themeal_db' as __source, current_timestamp() as __ingested_at, current_timestamp() as __updated_at
# MAGIC from country

# COMMAND ----------

# DBTITLE 1,recipe
# MAGIC %sql
# MAGIC select r.id_guid_sk, r.external_id_meal, r.meal as name_meal, c.id_guid_sk as guid_category, r.country, co.region as region, rf.instructions, rf.ingredient_measure, rf.image as image_recipe, rf.video as video_recipe, rf.source_url, rf.date_modified, 'themeal_db' as __source, current_timestamp() as __ingested_at, current_timestamp() as __updated_at
# MAGIC from recipe r 
# MAGIC join category c on c.name = r.category_name
# MAGIC join country co on r.country = co.country
# MAGIC join fully_recipe rf on r.external_id_meal = rf.external_id 