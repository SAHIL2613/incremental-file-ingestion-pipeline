# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 03 – Silver Layer
# MAGIC
# MAGIC ## Objective
# MAGIC Clean, transform, and validate Bronze data by removing duplicates, applying business rules, and performing incremental MERGE operations into the Silver Delta table.

# COMMAND ----------

# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame

# 1. Pre-create Main Silver Table & Quarantine Table
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.default.silver_transactions (
    transaction_id INT,
    transactional_date TIMESTAMP,
    product_id STRING,
    customer_id INT,
    payment STRING,
    credit_card STRING,
    loyalty_card STRING,
    cost DOUBLE,
    quantity INT,
    price DOUBLE,
    total_sales DOUBLE,
    file_name STRING,
    ingestion_time TIMESTAMP,
    processed_time TIMESTAMP,
    processed_flag INT
) USING DELTA;
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.default.silver_transactions_quarantine (
    transaction_id INT,
    raw_transactional_date STRING,
    product_id STRING,
    file_name STRING,
    quarantine_reason STRING,
    quarantine_time TIMESTAMP
) USING DELTA;
""")

# 2. Enhanced Micro-batch Handler with Quarantine Routing
def process_silver_microbatch(micro_batch_df: DataFrame, batch_id: int) -> None:
    if micro_batch_df.isEmpty():
        return

    # Apply Quality Rules & Parse Dates
    transformed_df = (
        micro_batch_df
        .filter(F.col("transaction_id").isNotNull())
        .dropDuplicates(["transaction_id"])
        .withColumn("raw_date_string", F.col("transactional_date"))
        .withColumn(
            "parsed_transactional_date",
            F.coalesce(
                F.try_to_timestamp(F.col("transactional_date"), F.lit("yyyy-MM-dd HH:mm:ss")),
                F.try_to_timestamp(F.col("transactional_date"), F.lit("M/d/yyyy H:mm")),
                F.try_to_timestamp(F.col("transactional_date"), F.lit("M/d/yyyy HH:mm")),
                F.try_to_timestamp(F.col("transactional_date"), F.lit("yyyy-MM-dd'T'HH:mm:ss")),
                F.try_to_timestamp(F.col("transactional_date"))
            )
        )
        .withColumn("total_sales", F.round(F.col("quantity") * F.col("price"), 2))
        .withColumn("processed_time", F.current_timestamp())
        .withColumn("processed_flag", F.lit(1))
    )

    # --- ROUTE BAD DATA TO QUARANTINE ---
    quarantine_records = (
        transformed_df
        .filter(F.col("parsed_transactional_date").isNull() & F.col("raw_date_string").isNotNull())
        .select(
            F.col("transaction_id"),
            F.col("raw_date_string").alias("raw_transactional_date"),
            F.col("product_id"),
            F.col("file_name"),
            F.lit("Unparseable Timestamp Format").alias("quarantine_reason"),
            F.current_timestamp().alias("quarantine_time")
        )
    )

    if not quarantine_records.isEmpty():
        quarantine_records.write.format("delta").mode("append").saveAsTable("workspace.default.silver_transactions_quarantine")
        print(f"⚠️ Quarantined {quarantine_records.count()} records due to invalid timestamp formatting.")

    # --- ROUTE CLEAN DATA TO SILVER ---
    valid_records = (
        transformed_df
        .filter(F.col("parsed_transactional_date").isNotNull())
        .withColumn("transactional_date", F.col("parsed_transactional_date"))
    )

    valid_records.createOrReplaceTempView("microbatch_silver_staging")

    spark.sql("""
        MERGE INTO workspace.default.silver_transactions AS target
        USING microbatch_silver_staging AS source
        ON target.transaction_id = source.transaction_id
        WHEN MATCHED THEN 
            UPDATE SET 
                target.transactional_date = source.transactional_date,
                target.product_id         = source.product_id,
                target.customer_id        = source.customer_id,
                target.payment            = source.payment,
                target.credit_card        = source.credit_card,
                target.loyalty_card       = source.loyalty_card,
                target.cost               = source.cost,
                target.quantity           = source.quantity,
                target.price              = source.price,
                target.total_sales        = source.total_sales,
                target.file_name          = source.file_name,
                target.ingestion_time     = source.ingestion_time,
                target.processed_time     = source.processed_time,
                target.processed_flag     = source.processed_flag
        WHEN NOT MATCHED THEN 
            INSERT (
                transaction_id, transactional_date, product_id, customer_id, 
                payment, credit_card, loyalty_card, cost, quantity, 
                price, total_sales, file_name, ingestion_time, 
                processed_time, processed_flag
            ) VALUES (
                source.transaction_id, source.transactional_date, source.product_id, source.customer_id, 
                source.payment, source.credit_card, source.loyalty_card, source.cost, source.quantity, 
                source.price, source.total_sales, source.file_name, source.ingestion_time, 
                source.processed_time, source.processed_flag
            );
    """)

# COMMAND ----------

display(spark.sql("SELECT count(*) FROM workspace.default.silver_transactions"))
display(spark.sql("SELECT transaction_id, transactional_date, total_sales FROM workspace.default.silver_transactions LIMIT 5"))