# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Bronze Layer Orchestrator
# MAGIC %md
# MAGIC # Bronze Layer Orchestrator
# MAGIC
# MAGIC This notebook orchestrates the bronze layer ingestion for multiple data sources:
# MAGIC - Orders (CSV)
# MAGIC - Customers (CSV)
# MAGIC - Reviews (JSON)

# COMMAND ----------

# DBTITLE 1,Import and Setup
import sys
import json

# Add src directory to path so we can import modules
sys.path.insert(0, '/Workspace/Users/panagiotiskoutridis@gmail.com/databricks-dynamic-pipeline/src')

# COMMAND ----------

# DBTITLE 1,Create Widget for Config
# Create widget for passing config to bronze ingestion
dbutils.widgets.text("config_json", "", "Config JSON")

# COMMAND ----------

# DBTITLE 1,Ingest Orders (Bronze)
# Configure and run orders ingestion
orders_config = {
    "source_name": "orders",
    "source_format": "csv",
    "source_path": "/Volumes/main/default/raw_landing/orders/",
    "read_options": {"header": "true"}
}

# Set widget value (remove first if exists)
try:
    dbutils.widgets.remove("config_json")
except:
    pass
dbutils.widgets.text("config_json", json.dumps(orders_config))

# Execute the bronze ingestion file
with open("/Workspace/Users/panagiotiskoutridis@gmail.com/databricks-dynamic-pipeline/src/01_bronze.py") as f:
    exec(f.read())

print(" Orders ingestion complete")

# COMMAND ----------

# DBTITLE 1,Ingest Customers (Bronze)
from pyspark.sql import functions as F
from utils.transforms import sanitize_column_names

# Batch ingest customers
df_raw = spark.read.option("header", "true").csv("/Volumes/main/default/raw_landing/customers/olist_customers.csv")
df_clean = sanitize_column_names(df_raw)
df_bronze = df_clean.withColumn("_ingested_at", F.current_timestamp())

df_bronze.write.format("delta").mode("overwrite").saveAsTable("main.bronze.customers")

print(f"Customers ingestion complete: {df_bronze.count()} rows")

# COMMAND ----------

# DBTITLE 1,Ingest Reviews (Bronze)
from pyspark.sql import functions as F
from utils.transforms import sanitize_column_names

# Batch ingest reviews (JSON format)
df_raw = spark.read.json("/Volumes/main/default/raw_landing/reviews/streaming_reviews_batch1.json")
df_clean = sanitize_column_names(df_raw)
df_bronze = df_clean.withColumn("_ingested_at", F.current_timestamp())

df_bronze.write.format("delta").mode("overwrite").saveAsTable("main.bronze.reviews")

print(f"Reviews ingestion complete: {df_bronze.count()} rows")

# COMMAND ----------

# DBTITLE 1,Verify All Bronze Tables
# MAGIC %sql
# MAGIC -- Verify all three bronze tables
# MAGIC SELECT 'orders' as table_name, COUNT(*) as row_count FROM main.bronze.orders
# MAGIC UNION ALL
# MAGIC SELECT 'customers' as table_name, COUNT(*) as row_count FROM main.bronze.customers
# MAGIC UNION ALL
# MAGIC SELECT 'reviews' as table_name, COUNT(*) as row_count FROM main.bronze.reviews

# COMMAND ----------

