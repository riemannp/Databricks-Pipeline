# Databricks notebook source

"""Delta Table Maintenance

Performs OPTIMIZE and ANALYZE operations on all Delta tables in the medallion architecture.
Optionally performs VACUUM (disabled by default for safety).
Runs after gold layer processing to maintain optimal query performance.
"""

import json
from datetime import datetime
from databricks.sdk.runtime import dbutils, spark

# Configuration
CATALOG = "main"
LAYERS = ["bronze", "silver", "gold"]
ENABLE_VACUUM = False  # Set to True only when you want to permanently remove old files
VACUUM_RETENTION_HOURS = 168  # 7 days (minimum recommended)
OPTIMIZE_WHERE_CLAUSE = None  # Set to filter optimization by partition if needed

print("=" * 70)
print("DELTA TABLE MAINTENANCE - OPTIMIZE & ANALYZE")
print("=" * 70)
print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Catalog: {CATALOG}")
print(f"Layers: {', '.join(LAYERS)}")
print(f"Vacuum enabled: {ENABLE_VACUUM}")
if ENABLE_VACUUM:
    print(f"Vacuum retention: {VACUUM_RETENTION_HOURS} hours")
print("=" * 70)

def get_tables_in_schema(catalog, schema):
    """Get all tables in a schema"""
    try:
        tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
        return [row.tableName for row in tables]
    except Exception as e:
        print(f"  ⚠ Warning: Could not access {catalog}.{schema}: {e}")
        return []

def optimize_table(full_table_name):
    """Run OPTIMIZE on a Delta table"""
    try:
        print(f"  ▶ OPTIMIZE {full_table_name}...", end=" ")
        
        optimize_sql = f"OPTIMIZE {full_table_name}"
        if OPTIMIZE_WHERE_CLAUSE:
            optimize_sql += f" WHERE {OPTIMIZE_WHERE_CLAUSE}"
        
        result = spark.sql(optimize_sql).collect()
        
        if result:
            metrics = result[0]
            files_added = getattr(metrics, 'num_files_added', 0)
            files_removed = getattr(metrics, 'num_files_removed', 0)
            print(f"✓ (removed {files_removed} files, added {files_added})")
        else:
            print("✓ completed")
        return True
        
    except Exception as e:
        print(f"✗ Error: {e}")
        return False

def vacuum_table(full_table_name, retention_hours):
    """Run VACUUM on a Delta table (only if enabled)"""
    if not ENABLE_VACUUM:
        print(f"  ⊗ VACUUM {full_table_name}... (skipped - disabled for safety)")
        return None
    
    try:
        print(f"  ▶ VACUUM {full_table_name}...", end=" ")
        vacuum_sql = f"VACUUM {full_table_name} RETAIN {retention_hours} HOURS"
        spark.sql(vacuum_sql)
        print("✓ completed")
        return True
    except Exception as e:
        print(f"✗ Error: {e}")
        return False

def analyze_table(full_table_name):
    """Update table statistics for query optimization"""
    try:
        print(f"  ▶ ANALYZE TABLE {full_table_name}...", end=" ")
        spark.sql(f"ANALYZE TABLE {full_table_name} COMPUTE STATISTICS")
        print("✓ completed")
        return True
    except Exception as e:
        print(f"✗ Error: {e}")
        return False

# Main maintenance workflow
maintenance_summary = {
    "optimized": 0,
    "vacuumed": 0,
    "analyzed": 0,
    "failed": 0,
    "total_tables": 0
}

for layer in LAYERS:
    schema = f"{CATALOG}.{layer}"
    print(f"\n{'=' * 70}")
    print(f"Processing schema: {schema}")
    print(f"{'=' * 70}")
    
    tables = get_tables_in_schema(CATALOG, layer)
    
    if not tables:
        print("  (no tables found)")
        continue
    
    print(f"Found {len(tables)} table(s): {', '.join(tables)}\n")
    
    for table_name in tables:
        full_table_name = f"{schema}.{table_name}"
        maintenance_summary["total_tables"] += 1
        
        print(f"\nMaintaining: {full_table_name}")
        
        # Step 1: OPTIMIZE (compact small files)
        if optimize_table(full_table_name):
            maintenance_summary["optimized"] += 1
        else:
            maintenance_summary["failed"] += 1
        
        # Step 2: ANALYZE (update statistics)
        if analyze_table(full_table_name):
            maintenance_summary["analyzed"] += 1
        
        # Step 3: VACUUM (remove old files) - optional
        vacuum_result = vacuum_table(full_table_name, VACUUM_RETENTION_HOURS)
        if vacuum_result is True:
            maintenance_summary["vacuumed"] += 1
        elif vacuum_result is False:
            maintenance_summary["failed"] += 1

# Final summary
print("\n" + "=" * 70)
print("MAINTENANCE SUMMARY")
print("=" * 70)
print(f"Total tables processed: {maintenance_summary['total_tables']}")
print(f"  ✓ Optimized: {maintenance_summary['optimized']}")
print(f"  ✓ Analyzed: {maintenance_summary['analyzed']}")
if ENABLE_VACUUM:
    print(f"  ✓ Vacuumed: {maintenance_summary['vacuumed']}")
if maintenance_summary['failed'] > 0:
    print(f"  ✗ Failed: {maintenance_summary['failed']}")

print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

# Store summary for monitoring
try:
    dbutils.jobs.taskValues.set(
        key="maintenance_summary",
        value=json.dumps(maintenance_summary)
    )
except Exception:
    pass  

if maintenance_summary['failed'] > 0:
    raise RuntimeError(
        f"Maintenance completed with {maintenance_summary['failed']} failures. "
        "Check logs above for details."
    )
