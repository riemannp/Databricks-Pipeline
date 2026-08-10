# Databricks notebook source

import json
from pyspark.sql import functions as F
from databricks.sdk.runtime import dbutils, spark
from utils.transforms import custom_transform_registry

try:
    raw_json = dbutils.widgets.get("config_json")
    if not raw_json or raw_json == "{}":
        raise ValueError("Empty configuration provided")
except Exception:
    print("INFO: No widget configuration found. Using default test configuration for 'orders'")
    raw_json = json.dumps({
        "source_name": "orders",
        "gold_transform": {
            "group_by": ["customer_id"],
            "aggregations": {"total_orders": "count(*)"}
        }
    })

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
    agg_exprs = []
    for alias, expr in agg_dict.items():
        if expr == "count(*)":
            agg_exprs.append(F.count("*").alias(alias))
        else:
            if ":" in expr:
                col, func = expr.split(":", 1)
                agg_exprs.append(F.expr(f"{func}({col})").alias(alias))
            else:
                agg_exprs.append(F.expr(expr).alias(alias))
    df_gold = df_silver.groupBy(*group_cols).agg(*agg_exprs)

df_gold.write.format("delta").mode("overwrite").saveAsTable(f"main.gold.{source_name}")