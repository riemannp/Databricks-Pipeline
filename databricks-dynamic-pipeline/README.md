# E-Commerce Medallion Data Pipeline

Production-grade ETL pipeline implementing the Medallion Architecture (Bronze-Silver-Gold) on Databricks, processing e-commerce transaction data with automated data quality checks and incremental processing.

## Architecture Overview

```
Raw Data Sources (CSV/JSON)
    ↓
Bronze Layer (Auto Loader ingestion)
    ↓
Silver Layer (Cleaned & transformed)
    ↓
Gold Layer (Business aggregates)
```

## Tech Stack

- **Platform**: Databricks on AWS
- **Compute**: Serverless (auto-scaling)
- **Storage**: Unity Catalog + Delta Lake
- **Orchestration**: Databricks Workflows (DAB deployment)
- **Languages**: Python, SQL, PySpark

## Key Features

### 1. Automated Data Ingestion
- **Auto Loader** for incremental file processing
- Schema evolution and validation
- Multi-format support (CSV, JSON)
- Automatic column name sanitization

### 2. Data Quality Framework
- Primary key validation with quarantine routing
- Duplicate detection and handling
- Rescued data capture for malformed records
- Comprehensive error logging

### 3. Incremental Processing
- CDC (Change Data Capture) merge patterns
- Streaming table updates
- State management with checkpoints
- Idempotent pipeline execution

### 4. Performance Optimization
- Automated OPTIMIZE operations
- Table statistics refresh (ANALYZE)
- Optional VACUUM for storage management
- Partition pruning support

## Project Structure

```
databricks-dynamic-pipeline/
├── config/
│   └── sources.json              # Source configurations
├── src/
│   ├── 00_config_exporter.py     # Config validation & export
│   ├── 01_bronze.py              # Raw data ingestion
│   ├── 02_silver.py              # Data cleaning & transformation
│   ├── 03_gold.py                # Business aggregations
│   ├── 04_maintenance.py         # Table optimization
│   └── utils/
│       └── transforms.py         # Reusable transformations
├── databricks.yml                # DAB deployment config
└── README.md
```

## Data Pipeline

### Bronze Layer
**Purpose**: Raw data ingestion with minimal transformation

- **Customers**: 99.4K records across 5 columns
- **Orders**: 99.4K records with order lifecycle timestamps
- **Reviews**: 99.2K records with ratings and comments

Key features:
- Automatic schema inference
- Column name sanitization
- Ingestion timestamp tracking
- Rescued data preservation

### Silver Layer
**Purpose**: Cleaned, validated, and enriched data

Transformations applied:
- Timestamp parsing and casting
- Null value handling with quarantine routing
- Derived metrics calculation (approval delays, delivery times)
- Text standardization (UPPER, TRIM)

**Data Quality**:
- Primary key validation
- Invalid records routed to `silver.quarantine`
- Quality metrics tracked per run

### Gold Layer
**Purpose**: Business-ready analytical aggregates

**Tables Created**:
1. `customer_order_summary` - Customer lifetime metrics
2. `order_performance_metrics` - Fulfillment KPIs
3. `geographic_analysis` - Regional performance
4. `review_analytics` - Sentiment analysis

**Sample Metrics**:
- Customer lifetime value
- Order approval/delivery delays
- Cancellation rates
- Average review scores by region

## Testing

### Run All Tests
```bash
# Quick test run
./run_tests.sh fast

# With coverage report
./run_tests.sh all

# CI/CD validation
./run_tests.sh ci
```

**Test Coverage**: 80%+ across all layers  
**Test Suite**: 100+ unit tests covering configuration, transformations, and business logic

See [tests/README.md](tests/README.md) for complete documentation.

---

## Deployment

### Prerequisites
```bash
pip install -r requirements.txt
databricks configure --token
```

### Deploy Pipeline
```bash
# Validate configuration
databricks bundle validate --strict

# Deploy to dev environment
databricks bundle deploy --target dev

# Run pipeline
databricks jobs run-now --job-id <job_id>
```

**CI/CD**: Automated deployment on merge to main. See [CI_CD_SETUP_GUIDE.md](CI_CD_SETUP_GUIDE.md).

### Pipeline Schedule
- **Frequency**: Daily at 2:00 AM UTC
- **Timeout**: 2 hours
- **Retry Policy**: 2-3 retries per task
- **Notifications**: Email alerts on start/success/failure

## Configuration

Source configurations in `config/sources.json`:

```json
{
  "source_name": "orders",
  "source_format": "csv",
  "source_path": "/Volumes/main/default/raw_landing/orders/",
  "silver_transform": {
    "mode": "cdc_merge",
    "primary_key": "order_id"
  }
}
```

## Data Quality Metrics

**Bronze → Silver Success Rate**: 99.8%
- Invalid records quarantined: ~200 rows
- Primary key violations: < 0.1%
- Schema evolution events: 0

**Pipeline Reliability**:
- End-to-end execution time: ~12 minutes
- Table optimization: Automated post-processing
- Zero data loss (rescued data captured)

## Monitoring & Observability

- Task-level timeout configurations
- Email notifications for pipeline status
- Delta table history tracking
- Checkpoint-based recovery

## Lessons Learned

1. **Schema Management**: Auto Loader's schema evolution required careful handling when multiple CSV files with different headers existed in the same folder
2. **State Recovery**: Checkpoint deletion was necessary after schema conflicts to allow clean re-ingestion
3. **Column Naming**: Implemented exhaustive deduplication in `sanitize_column_names()` to prevent duplicate column issues

## Future Enhancements

- [ ] Add data lineage tracking
- [ ] Implement SCD Type 2 for dimension tables
- [ ] Add ML model for review sentiment analysis
- [ ] Create real-time dashboard with streaming metrics

## Contact

**Panagiotis Koutridis**  
[panagiotiskoutridis@gmail.com](mailto:panagiotiskoutridis@gmail.com)

---

*Built with Databricks on AWS | Delta Lake | Unity Catalog*
