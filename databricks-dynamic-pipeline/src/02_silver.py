# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
import json

from databricks.sdk.runtime import dbutils, spark
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# Configuration - can be provided via widget or uses default for testing
try:
    raw_json = dbutils.widgets.get("config_json")
    if not raw_json or raw_json == "{}":
        raise ValueError("Empty configuration provided")
except Exception:
    # Fallback to default configuration for direct execution/testing
    print("INFO: No widget configuration found. Using default test configuration for 'orders'")
    raw_json = json.dumps({
        "source_name": "orders",
        "silver_transform": {
            "mode": "overwrite",
            "drop_nulls": ["order_id"]
        }
    })

cfg = json.loads(raw_json)
source_name = cfg.get("source_name")
transform_cfg = cfg.get("silver_transform", {})

if not source_name:
    raise ValueError(
        "Missing 'source_name' in configuration. "
        "Please provide a valid source name: orders, customers, or reviews"
    )

df_bronze = spark.table(f"main.bronze.{source_name}")

# Data Quality Check: Route invalid primary key records to Quarantine
drop_cols = transform_cfg.get("drop_nulls", [])
if drop_cols:
    null_condition = " OR ".join([f"{c} IS NULL" for c in drop_cols])
    df_quarantine = df_bronze.filter(null_condition).withColumn("_quarantine_reason", F.lit("Missing primary key"))
    df_quarantine.write.format("delta").mode("append").saveAsTable("main.silver.quarantine")
    df_clean = df_bronze.filter(f"NOT ({null_condition})")
else:
    df_clean = df_bronze

# Apply business transformations based on source
if source_name == "orders":
    # Parse date columns and add derived metrics
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

elif source_name == "reviews":
    # Parse review timestamps
    df_clean = (df_clean
        .withColumn("review_creation_date", F.to_timestamp("review_creation_date"))
        .withColumn("review_answer_timestamp", F.to_timestamp("review_answer_timestamp"))
        .withColumn("review_score", F.col("review_score").cast("int"))
        .withColumn("review_length", F.length(F.col("review_comment_message")))
    )

elif source_name == "customers":
    # Standardize customer data
    df_clean = (df_clean
        .withColumn("customer_city", F.upper(F.trim("customer_city")))
        .withColumn("customer_state", F.upper(F.trim("customer_state")))
    )

# Stateful CDC Merge vs Standard Write
if transform_cfg.get("mode") == "cdc_merge":
    pk = transform_cfg["primary_key"]
    seq = transform_cfg["sequence_col"]

    window_spec = F.Window.partitionBy(pk).orderBy(F.col(seq).desc())
    deduped_df = df_clean.withColumn("_rn", F.row_number().over(window_spec)).filter("_rn = 1").drop("_rn")

    if spark.catalog.tableExists(f"main.silver.{source_name}"):
        target = DeltaTable.forName(spark, f"main.silver.{source_name}")
        target.alias("t").merge(
            deduped_df.alias("s"),
            f"t.{pk} = s.{pk}"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        deduped_df.write.format("delta").mode("overwrite").saveAsTable(f"main.silver.{source_name}")
else:
    df_clean.write.format("delta").mode("overwrite").saveAsTable(f"main.silver.{source_name}")
