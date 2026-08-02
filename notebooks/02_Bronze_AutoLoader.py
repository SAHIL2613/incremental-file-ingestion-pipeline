# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 02 – Bronze Layer
# MAGIC
# MAGIC ## Objective
# MAGIC Ingest raw transaction files incrementally into the Bronze Delta table using Databricks Auto Loader with schema evolution and checkpointing.

# COMMAND ----------

# Databricks notebook source
from pyspark.sql import functions as F

# 1. Define Volume & Checkpoint Paths
landing_path = "/Volumes/workspace/default/raw_landing_data/"
checkpoint_path = "/Volumes/workspace/default/raw_landing_data/_checkpoints/bronze_chkpt"
schema_path = "/Volumes/workspace/default/raw_landing_data/_checkpoints/bronze_schema"

# 2. Ensure target Bronze Delta Table exists
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.default.bronze_transactions (
    transaction_id INT,
    transactional_date STRING,
    product_id STRING,
    customer_id INT,
    payment STRING,
    credit_card STRING,
    loyalty_card STRING,
    cost DOUBLE,
    quantity INT,
    price DOUBLE,
    file_name STRING,
    file_arrival_time TIMESTAMP,
    ingestion_time TIMESTAMP
) USING DELTA;
""")

# 3. Define Auto Loader Read Stream (cloudFiles)
bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(landing_path)
    # Metadata Enrichment for lineage tracking
    .withColumn("file_name", F.col("_metadata.file_path"))
    .withColumn("file_arrival_time", F.col("_metadata.file_modification_time"))
    .withColumn("ingestion_time", F.current_timestamp())
)

# 4. Execute Write Stream with mergeSchema = true (Serverless compatible)
query_bronze = (
    bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("mergeSchema", "true")  # Dynamic schema evolution
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable("workspace.default.bronze_transactions")
)

# Wait for stream micro-batch to complete
query_bronze.awaitTermination()
print(" Bronze Ingestion Stream Completed Successfully!")

# COMMAND ----------

# Databricks notebook source
# 1. Row count verification
total_bronze_records = spark.sql("SELECT COUNT(*) AS total_count FROM workspace.default.bronze_transactions").collect()[0]["total_count"]
print(f"Total Bronze Records: {total_bronze_records}")

# 2. Preview Bronze records with operational lineage
display(spark.sql("""
    SELECT 
        transaction_id, 
        transactional_date, 
        product_id, 
        customer_id, 
        cost, 
        quantity, 
        price, 
        file_name, 
        ingestion_time 
    FROM workspace.default.bronze_transactions 
    LIMIT 5
"""))