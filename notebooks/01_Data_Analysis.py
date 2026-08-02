# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental File Ingestion Pipeline using Auto Loader & Delta Lake
# MAGIC
# MAGIC ## Notebook 01 – Data Analysis
# MAGIC
# MAGIC ### Objective
# MAGIC Analyze the incoming raw datasets before ingestion into the Bronze layer.
# MAGIC This notebook validates file availability, inspects schema, profiles data quality,
# MAGIC and identifies issues such as null values, duplicate records, and inconsistent data types.

# COMMAND ----------

from pyspark.sql import functions as F

# --------------------------------------------
# Configuration
# --------------------------------------------

RAW_DATA_PATH = "/Volumes/workspace/default/raw_landing_data"

# COMMAND ----------

print("=" * 60)
print("VALIDATING RAW LANDING ZONE")
print("=" * 60)

files = dbutils.fs.ls(RAW_DATA_PATH)

display(files)

print(f"Total files found: {len(files)}")

# COMMAND ----------

fact_sales_1 = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{RAW_DATA_PATH}/Fact_Sales_1.csv")
)

fact_sales_2 = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{RAW_DATA_PATH}/Fact_Sales_2.csv")
)

sales_2010_12_08 = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{RAW_DATA_PATH}/2010-12-08.csv")
)

# COMMAND ----------

datasets = {
    "Fact_Sales_1": fact_sales_1,
    "Fact_Sales_2": fact_sales_2,
    "Sales_2010_12_08": sales_2010_12_08
}

for name, df in datasets.items():

    print("=" * 60)
    print(name)
    print("=" * 60)

    print(f"Rows    : {df.count()}")
    print(f"Columns : {len(df.columns)}")

    df.printSchema()

    display(df.limit(5))

# COMMAND ----------

for name, df in datasets.items():

    print(f"\n{name}")

    null_df = df.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(c)
        for c in df.columns
    ])

    display(null_df)

# COMMAND ----------

for name, df in datasets.items():

    duplicates = df.count() - df.dropDuplicates().count()

    print(f"{name} Duplicate Records : {duplicates}")

# COMMAND ----------

for name, df in datasets.items():

    print(f"\n{name}")

    display(df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Observations
# MAGIC
# MAGIC - All source files were successfully detected.
# MAGIC - Schemas were inferred correctly.
# MAGIC - Duplicate records were identified.
# MAGIC - Null value distribution was analyzed.
# MAGIC - Dataset is ready for Bronze ingestion.