# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Silver Layer Orchestrator
# MAGIC %md
# MAGIC # Silver Layer Orchestrator
# MAGIC
# MAGIC This notebook transforms bronze tables into clean, business-ready silver tables:
# MAGIC - **Orders**: Deduplicate, validate, add derived metrics
# MAGIC - **Customers**: Standardize, clean nulls
# MAGIC - **Reviews**: Parse timestamps, sentiment preparation

# COMMAND ----------

# DBTITLE 1,Import and Setup
import sys
import json
from pyspark.sql import functions as F

# Add src directory to path
sys.path.insert(0, '/Workspace/Users/panagiotiskoutridis@gmail.com/databricks-dynamic-pipeline/src')

# COMMAND ----------

# DBTITLE 1,Create Config Widget
# Create widget for passing config to silver transformation
dbutils.widgets.text("config_json", "", "Config JSON")

# COMMAND ----------

# DBTITLE 1,Transform Orders to Silver
# Configure orders transformation with CDC merge
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

source_name = "orders"
transform_cfg = {
    "mode": "cdc_merge",
    "primary_key": "order_id",
    "sequence_col": "_ingested_at",
    "drop_nulls": ["order_id"]
}

df_bronze = spark.table(f"main.bronze.{source_name}")

# Data Quality Check: Route invalid primary key records to Quarantine
drop_cols = transform_cfg.get("drop_nulls", [])
if drop_cols:
    null_condition = " OR ".join([f"{c} IS NULL" for c in drop_cols])
    df_quarantine = (df_bronze.filter(null_condition)
        .withColumn("_quarantine_reason", F.lit("Missing primary key"))
        .withColumn("_source_table", F.lit(source_name)))
    if not spark.catalog.tableExists("main.silver.quarantine"):
        df_quarantine.write.format("delta").mode("overwrite").saveAsTable("main.silver.quarantine")
    else:
        df_quarantine.write.format("delta").mode("append").saveAsTable("main.silver.quarantine")
    df_clean = df_bronze.filter(f"NOT ({null_condition})")
else:
    df_clean = df_bronze

# Apply business transformations for orders
df_clean = (df_clean
    .withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))
    .withColumn("order_approved_at", F.to_timestamp("order_approved_at"))
    .withColumn("order_delivered_carrier_date", F.to_timestamp("order_delivered_carrier_date"))
    .withColumn("order_delivered_customer_date", F.to_timestamp("order_delivered_customer_date"))
    .withColumn("order_estimated_delivery_date", F.to_timestamp("order_estimated_delivery_date"))
    .withColumn("approval_delay_hours", 
                F.round((F.unix_timestamp("order_approved_at") - F.unix_timestamp("order_purchase_timestamp")) / 3600, 2))
    .withColumn("delivery_delay_days",
                F.datediff("order_delivered_customer_date", "order_estimated_delivery_date"))
)

# CDC Merge
pk = transform_cfg["primary_key"]
seq = transform_cfg["sequence_col"]

window_spec = Window.partitionBy(pk).orderBy(F.col(seq).desc())
deduped_df = df_clean.withColumn("_rn", F.row_number().over(window_spec)).filter("_rn = 1").drop("_rn")

if spark.catalog.tableExists(f"main.silver.{source_name}"):
    target = DeltaTable.forName(spark, f"main.silver.{source_name}")
    target.alias("t").merge(
        deduped_df.alias("s"),
        f"t.{pk} = s.{pk}"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    deduped_df.write.format("delta").mode("overwrite").saveAsTable(f"main.silver.{source_name}")

print(f"✅ Orders silver transformation complete: {deduped_df.count()} rows")

# COMMAND ----------

# DBTITLE 1,Transform Customers to Silver
# Configure customers transformation
from pyspark.sql import functions as F

source_name = "customers"
transform_cfg = {
    "mode": "overwrite",
    "drop_nulls": ["customer_id"]
}

df_bronze = spark.table(f"main.bronze.{source_name}")

# Data Quality Check: Route invalid primary key records to Quarantine
drop_cols = transform_cfg.get("drop_nulls", [])
if drop_cols:
    null_condition = " OR ".join([f"{c} IS NULL" for c in drop_cols])
    df_quarantine = (df_bronze.filter(null_condition)
        .withColumn("_quarantine_reason", F.lit("Missing primary key"))
        .withColumn("_source_table", F.lit(source_name)))
    # Use schema merge option to handle different source schemas
    df_quarantine.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("main.silver.quarantine")
    df_clean = df_bronze.filter(f"NOT ({null_condition})")
else:
    df_clean = df_bronze

# Apply business transformations for customers
df_clean = (df_clean
    .withColumn("customer_city", F.upper(F.trim("customer_city")))
    .withColumn("customer_state", F.upper(F.trim("customer_state")))
)

# Overwrite mode
df_clean.write.format("delta").mode("overwrite").saveAsTable(f"main.silver.{source_name}")

print(f"✅ Customers silver transformation complete: {df_clean.count()} rows")

# COMMAND ----------

# DBTITLE 1,Transform Reviews to Silver
# Configure reviews transformation with CDC merge
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

source_name = "reviews"
transform_cfg = {
    "mode": "cdc_merge",
    "primary_key": "review_id",
    "sequence_col": "_ingested_at",
    "drop_nulls": ["review_id"]
}

df_bronze = spark.table(f"main.bronze.{source_name}")

# Data Quality Check: Route invalid primary key records to Quarantine
drop_cols = transform_cfg.get("drop_nulls", [])
if drop_cols:
    null_condition = " OR ".join([f"{c} IS NULL" for c in drop_cols])
    df_quarantine = (df_bronze.filter(null_condition)
        .withColumn("_quarantine_reason", F.lit("Missing primary key"))
        .withColumn("_source_table", F.lit(source_name)))
    # Use schema merge option to handle different source schemas
    df_quarantine.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("main.silver.quarantine")
    df_clean = df_bronze.filter(f"NOT ({null_condition})")
else:
    df_clean = df_bronze

# Apply business transformations for reviews
df_clean = (df_clean
    .withColumn("review_creation_date", F.to_timestamp("review_creation_date"))
    .withColumn("review_answer_timestamp", F.to_timestamp("review_answer_timestamp"))
    .withColumn("review_score", F.col("review_score").cast("int"))
    .withColumn("review_length", F.length(F.col("review_comment_message")))
)

# CDC Merge
pk = transform_cfg["primary_key"]
seq = transform_cfg["sequence_col"]

window_spec = Window.partitionBy(pk).orderBy(F.col(seq).desc())
deduped_df = df_clean.withColumn("_rn", F.row_number().over(window_spec)).filter("_rn = 1").drop("_rn")

if spark.catalog.tableExists(f"main.silver.{source_name}"):
    target = DeltaTable.forName(spark, f"main.silver.{source_name}")
    target.alias("t").merge(
        deduped_df.alias("s"),
        f"t.{pk} = s.{pk}"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    deduped_df.write.format("delta").mode("overwrite").saveAsTable(f"main.silver.{source_name}")

print(f"✅ Reviews silver transformation complete: {deduped_df.count()} rows")

# COMMAND ----------

# DBTITLE 1,Verify All Silver Tables
# MAGIC %sql
# MAGIC -- Verify all silver tables
# MAGIC SELECT 'orders' as table_name, COUNT(*) as row_count FROM main.silver.orders
# MAGIC UNION ALL
# MAGIC SELECT 'customers' as table_name, COUNT(*) as row_count FROM main.silver.customers
# MAGIC UNION ALL
# MAGIC SELECT 'reviews' as table_name, COUNT(*) as row_count FROM main.silver.reviews
# MAGIC UNION ALL
# MAGIC SELECT 'quarantine' as table_name, COUNT(*) as row_count FROM main.silver.quarantine

# COMMAND ----------

# DBTITLE 1,Sample Silver Data Quality
# MAGIC %sql
# MAGIC -- Sample orders with derived metrics
# MAGIC SELECT 
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   order_status,
# MAGIC   order_purchase_timestamp,
# MAGIC   approval_delay_hours,
# MAGIC   delivery_delay_days
# MAGIC FROM main.silver.orders 
# MAGIC LIMIT 5

# COMMAND ----------

# DBTITLE 1,Sample Customers Data
# MAGIC %sql
# MAGIC -- Sample customers with standardized cities/states
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   customer_city,
# MAGIC   customer_state,
# MAGIC   customer_zip_code_prefix
# MAGIC FROM main.silver.customers
# MAGIC LIMIT 5

# COMMAND ----------

# DBTITLE 1,Sample Reviews Data
# MAGIC %sql
# MAGIC -- Sample reviews with parsed timestamps and derived metrics
# MAGIC SELECT 
# MAGIC   review_id,
# MAGIC   order_id,
# MAGIC   review_score,
# MAGIC   review_length,
# MAGIC   review_creation_date,
# MAGIC   review_comment_title
# MAGIC FROM main.silver.reviews
# MAGIC LIMIT 5

# COMMAND ----------

