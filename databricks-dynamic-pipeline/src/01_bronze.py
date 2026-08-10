# Databricks notebook source

import sys
import json
from pyspark.sql import functions as F
from databricks.sdk.runtime import dbutils, spark
from utils.transforms import sanitize_column_names

# Get config_json from widget (notebook exec) or command line (DAB task)
try:
    raw_json = dbutils.widgets.get("config_json")
except Exception:
    raw_json = None

if not raw_json or raw_json.strip() == '':
    # sys.argv[1] might be a flag like '-f' in direct file execution, not config
    if len(sys.argv) > 1 and not sys.argv[1].startswith('-'):
        raw_json = sys.argv[1]
    else:
        raw_json = None

if not raw_json or not raw_json.strip():
    # Fallback test config for standalone debugging
    print("WARNING: No config provided. Using test config for 'orders' source.")
    raw_json = json.dumps({
        "source_name": "orders",
        "source_format": "csv",
        "source_path": "/Volumes/main/default/raw_landing/orders/",
        "read_options": {"header": "true"}
    })

print(f"DEBUG: raw_json = {repr(raw_json[:200] if len(raw_json) > 200 else raw_json)}")

try:
    cfg = json.loads(raw_json)
except json.JSONDecodeError as e:
    raise ValueError(f"Invalid JSON config: {e}. Received: {repr(raw_json[:100])}")

if "source_name" not in cfg:
    raise ValueError(f"Config missing 'source_name' field. Received config: {cfg}")

print(f"DEBUG: Parsed config for source: {cfg['source_name']}")
source_name = cfg["source_name"]

# Add file extension filter to avoid reading wrong file types
source_path = cfg["source_path"]
if source_path.endswith("/"):
    source_path = source_path + f"*.{cfg['source_format']}"

df_raw = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", cfg["source_format"])
    .option("cloudFiles.schemaLocation", f"/Volumes/main/default/raw_landing/_schemas/{source_name}")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .options(**cfg.get("read_options", {}))
    .load(source_path)
)

df_clean = sanitize_column_names(df_raw)
df_bronze = df_clean.withColumn("_ingested_at", F.current_timestamp())

(
    df_bronze.writeStream.format("delta")
    .option("checkpointLocation", f"/Volumes/main/default/raw_landing/_checkpoints/{source_name}")
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(f"main.bronze.{source_name}")
).awaitTermination()