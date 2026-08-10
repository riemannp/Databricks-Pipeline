# Databricks notebook source

"""Configuration Exporter

Reads sources.json and exports configurations for downstream pipeline tasks.
This task runs first in the DAB job to validate and prepare configurations.
"""

import json
import sys
from pathlib import Path
from databricks.sdk.runtime import dbutils, spark

# Determine config file path based on execution context
config_path = "/Workspace/Users/panagiotiskoutridis@gmail.com/Databricks-Pipeline/databricks-dynamic-pipeline/config/sources.json"

try:
    # Try DBFS path first (when running as file)
    with open(config_path.replace("/Workspace", "/dbfs/Users")) as f:
        sources = json.load(f)
except FileNotFoundError:
    try:
        # Try workspace path (when running as notebook)
        with open(config_path) as f:
            sources = json.load(f)
    except FileNotFoundError:
        # Last resort: use dbutils to read
        content = dbutils.fs.head(config_path.replace("/Workspace", ""))
        sources = json.loads(content)

print("=" * 70)
print("CONFIG EXPORTER - Pipeline Configuration Loaded")
print("=" * 70)
print(f"Total sources: {len(sources)}")
print()

# Validate configuration structure
required_fields = ["source_name", "layer"]
for idx, source in enumerate(sources):
    missing_fields = [f for f in required_fields if f not in source]
    if missing_fields:
        raise ValueError(
            f"Source at index {idx} missing required fields: {missing_fields}\n"
            f"Source: {source}"
        )

# Group sources by layer
by_layer = {}
for source in sources:
    layer = source["layer"]
    if layer not in by_layer:
        by_layer[layer] = []
    by_layer[layer].append(source)

# Display configuration summary
print("Configuration Summary:")
for layer, layer_sources in by_layer.items():
    print(f"\n  {layer}:")
    for source in layer_sources:
        print(f"    - {source['source_name']}")

print("\n" + "=" * 70)
print("Exporting configurations to dbutils.jobs.taskValues")
print("=" * 70)

# Export for downstream tasks to consume
try:
    # Store full config as task value for other tasks to reference
    dbutils.jobs.taskValues.set(key="pipeline_config", value=json.dumps(sources))
    
    # Export source names by layer for easy filtering
    bronze_sources = [s["source_name"] for s in sources if s["layer"] == "bronze_to_silver"]
    gold_sources = [s["source_name"] for s in sources if "gold" in s["layer"]]
    
    dbutils.jobs.taskValues.set(key="bronze_sources", value=json.dumps(bronze_sources))
    dbutils.jobs.taskValues.set(key="gold_sources", value=json.dumps(gold_sources))
    
    print(f"✓ Exported {len(bronze_sources)} bronze sources")
    print(f"✓ Exported {len(gold_sources)} gold sources")
    print(f"✓ Pipeline configuration ready for downstream tasks")
    
except Exception as e:
    print(f"⚠ Warning: Could not set task values (may not be running in job context): {e}")
    print("   Continuing anyway - config validation successful")

print("\n" + "=" * 70)
print("✓ Configuration export complete")
print("=" * 70)