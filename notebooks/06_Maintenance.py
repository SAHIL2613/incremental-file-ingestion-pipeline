# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 06 – Maintenance & Optimization
# MAGIC
# MAGIC ## Objective
# MAGIC Maintain Delta Lake tables by optimizing storage, cleaning obsolete files, verifying table health, and collecting maintenance information to ensure long-term performance and reliability.

# COMMAND ----------

BRONZE_TABLE = "workspace.default.bronze_transactions"
SILVER_TABLE = "workspace.default.silver_transactions"
GOLD_TABLE = "workspace.default.gold_kpi_metrics"

# COMMAND ----------

print("===== Table Counts =====")

tables = [BRONZE_TABLE, SILVER_TABLE, GOLD_TABLE]

for table in tables:
    count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {table}").collect()[0]["cnt"]
    print(f"{table}: {count} records")

# COMMAND ----------

print("===== Bronze History =====")
display(spark.sql(f"DESCRIBE HISTORY {BRONZE_TABLE}"))

print("===== Silver History =====")
display(spark.sql(f"DESCRIBE HISTORY {SILVER_TABLE}"))

print("===== Gold History =====")
display(spark.sql(f"DESCRIBE HISTORY {GOLD_TABLE}"))

# COMMAND ----------

spark.sql(f"OPTIMIZE {BRONZE_TABLE}")
spark.sql(f"OPTIMIZE {SILVER_TABLE}")
spark.sql(f"OPTIMIZE {GOLD_TABLE}")

print("Optimization completed successfully.")

# COMMAND ----------


spark.sql(f"VACUUM {BRONZE_TABLE} RETAIN 168 HOURS")
spark.sql(f"VACUUM {SILVER_TABLE} RETAIN 168 HOURS")
spark.sql(f"VACUUM {GOLD_TABLE} RETAIN 168 HOURS")

print("Vacuum completed successfully.")

# COMMAND ----------

print("===== Bronze Details =====")
display(spark.sql(f"DESCRIBE DETAIL {BRONZE_TABLE}"))

print("===== Silver Details =====")
display(spark.sql(f"DESCRIBE DETAIL {SILVER_TABLE}"))

print("===== Gold Details =====")
display(spark.sql(f"DESCRIBE DETAIL {GOLD_TABLE}"))

# COMMAND ----------

bronze = spark.sql(f"SELECT COUNT(*) cnt FROM {BRONZE_TABLE}").first()["cnt"]
silver = spark.sql(f"SELECT COUNT(*) cnt FROM {SILVER_TABLE}").first()["cnt"]
gold = spark.sql(f"SELECT COUNT(*) cnt FROM {GOLD_TABLE}").first()["cnt"]

print("=" * 50)
print("Pipeline Maintenance Summary")
print("=" * 50)
print(f"Bronze Records : {bronze}")
print(f"Silver Records : {silver}")
print(f"Gold Records   : {gold}")
print("Status         : SUCCESS")
print("=" * 50)