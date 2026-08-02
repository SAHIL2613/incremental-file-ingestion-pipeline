# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 05 – Audit & Monitoring
# MAGIC
# MAGIC ## Objective
# MAGIC Monitor pipeline execution, validate Delta table operations, and review processing history to ensure pipeline reliability and operational health.

# COMMAND ----------

# Databricks notebook source

print("=== 1. Pipeline Summary across Medallion Layers ===")
display(spark.sql("""
    SELECT 'Bronze' AS layer, COUNT(*) AS record_count FROM workspace.default.bronze_transactions
    UNION ALL
    SELECT 'Silver' AS layer, COUNT(*) AS record_count FROM workspace.default.silver_transactions
    UNION ALL
    SELECT 'Gold' AS layer, total_records_processed AS record_count FROM workspace.default.gold_kpi_metrics
"""))

print("=== 2. Silver Delta Table Commit History ===")
display(spark.sql("DESCRIBE HISTORY workspace.default.silver_transactions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Expected Output
# MAGIC - Delta transaction history.
# MAGIC - Pipeline execution validation.
# MAGIC - Operational insights for monitoring and maintenance.