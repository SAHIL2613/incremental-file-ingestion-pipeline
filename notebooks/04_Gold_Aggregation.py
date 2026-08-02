# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 04 – Gold Layer
# MAGIC
# MAGIC ## Objective
# MAGIC Generate business KPIs from the Silver layer by aggregating processed transaction data into the Gold Delta table for reporting and analytics.

# COMMAND ----------

# Databricks notebook source
from pyspark.sql import functions as F
    
# 1. Define Checkpoint Location
gold_checkpoint_path = "/Volumes/workspace/default/raw_landing_data/_checkpoints/gold_chkpt"

# 2. Pre-create Managed Gold Table
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.default.gold_kpi_metrics (
    processed_flag INT,
    total_records_processed BIGINT,
    total_revenue DOUBLE,
    latest_data_freshness TIMESTAMP,
    calculation_time TIMESTAMP
) USING DELTA;
""")

# 3. Read Stream from Silver Layer
silver_stream = spark.readStream.table("workspace.default.silver_transactions")

# 4. Define KPI Aggregations
gold_aggregated = (
    silver_stream
    .groupBy("processed_flag")
    .agg(
        F.count("transaction_id").alias("total_records_processed"),
        F.round(F.sum("total_sales"), 2).alias("total_revenue"),
        F.max("processed_time").alias("latest_data_freshness")
    )
    .withColumn("calculation_time", F.current_timestamp())
)

# 5. Write Complete Aggregations to Gold Delta Table
gold_query = (
    gold_aggregated.writeStream
    .format("delta")
    .outputMode("complete")  # Recomputes aggregate for the batch
    .option("checkpointLocation", gold_checkpoint_path)
    .trigger(availableNow=True)
    .toTable("workspace.default.gold_kpi_metrics")
)

gold_query.awaitTermination()
print("✅ Gold Layer KPI Aggregations Completed Successfully!")

# COMMAND ----------

# Databricks notebook source
display(spark.sql("SELECT * FROM workspace.default.gold_kpi_metrics"))