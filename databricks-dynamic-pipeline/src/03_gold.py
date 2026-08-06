import json
from pyspark.sql import functions as F
from databricks.sdk.runtime import dbutils, spark
from utils.transforms import custom_transform_registry

raw_json = dbutils.widgets.get("config_json")
cfg = json.loads(raw_json)
source_name = cfg["source_name"]
gold_cfg = cfg.get("gold_transform", {})

if gold_cfg.get("mode") == "custom_python":
    fn = custom_transform_registry[gold_cfg["function_name"]]
    df_gold = fn(spark)
else:
    df_silver = spark.table(f"main.silver.{source_name}")
    group_cols = gold_cfg.get("group_by", [])
    agg_dict = gold_cfg.get("aggregations", {})
    df_gold = df_silver.groupBy(*group_cols).agg(agg_dict)

df_gold.write.format("delta").mode("overwrite").saveAsTable(f"main.gold.{source_name}")