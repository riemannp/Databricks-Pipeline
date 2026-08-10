# Test Suite Documentation

## Overview

Comprehensive test suite for the E-Commerce Medallion Data Pipeline, covering configuration validation, transformation logic, and data quality checks across Bronze, Silver, and Gold layers.

## Test Files

### 1. **test_config.py** - Configuration Validation
**Coverage**: Configuration structure and validation

**Test Classes:**
* `TestSourcesConfigStructure` - Config file existence and JSON validity
* `TestSourceConfigFields` - Required field validation
* `TestBronzeToSilverSources` - Bronze layer source configuration
* `TestGoldCustomSources` - Gold layer custom transform configuration
* `TestSourceNameUniqueness` - Duplicate source name detection
* `TestSpecificSources` - Known source verification (orders, customers, reviews)

**Key Tests:**
* Config file exists and contains valid JSON
* All sources have required fields (source_name, layer, etc.)
* Layer values are valid (bronze_to_silver, gold_custom)
* CDC merge sources have primary_key configured
* Source paths point to Unity Catalog volumes
* Source names are unique

---

### 2. **test_transforms.py** - Transformation Functions
**Coverage**: Utility transformation functions

**Test Classes:**
* `TestSanitizeColumnNames` - Column name sanitization logic
* `TestCustomTransformRegistry` - Transform registry validation
* `TestJoinEcomDomain` - Gold layer join function

**Key Tests:**
* Basic column name cleaning (spaces, hyphens, case)
* Duplicate column name handling with suffixes
* Whitespace trimming and special character handling
* Data preservation during transformation
* Transform registry structure and function registration

---

### 3. **test_bronze_logic.py** - Bronze Layer Ingestion
**Coverage**: Bronze layer data ingestion logic

**Test Classes:**
* `TestConfigParsing` - Config JSON parsing
* `TestPathConstruction` - Source path building
* `TestIngestionTimestampColumn` - Timestamp column addition
* `TestColumnSanitization` - Integration with transforms
* `TestCheckpointAndSchemaLocations` - Path construction
* `TestOutputTableNaming` - Bronze table naming conventions

**Key Tests:**
* Valid/invalid JSON config parsing
* Missing source_name detection
* Path wildcard construction for different formats
* _ingested_at timestamp column addition
* Checkpoint and schema location path uniqueness
* Bronze table naming follows main.bronze.{source_name}

---

### 4. **test_silver_logic.py** - Silver Layer Transformations
**Coverage**: Silver layer data cleaning and enrichment

**Test Classes:**
* `TestSilverConfigParsing` - Silver config parsing
* `TestDataQualityQuarantine` - Null handling and quarantine
* `TestOrdersTransformations` - Orders-specific logic
* `TestReviewsTransformations` - Reviews-specific logic
* `TestCustomersTransformations` - Customers-specific logic
* `TestCDCMergeLogic` - CDC merge deduplication

**Key Tests:**
* Silver transform config parsing (mode, drop_nulls)
* Null primary key filtering and quarantine routing
* Timestamp parsing for orders (purchase, approval, delivery)
* Approval delay calculation in hours
* Delivery delay calculation in days
* Review score casting and length calculation
* Customer city/state standardization (UPPER, TRIM)
* CDC deduplication keeps latest record per primary key

---

### 5. **test_gold_logic.py** - Gold Layer Aggregations
**Coverage**: Gold layer business metric calculations

**Test Classes:**
* `TestCustomerOrderSummary` - Customer-level aggregations
* `TestOrderPerformanceMetrics` - Order performance KPIs
* `TestGeographicAnalysis` - Regional aggregations
* `TestReviewAnalytics` - Review metrics
* `TestJoinOperations` - Multi-table joins

**Key Tests:**
* Customer order count (distinct)
* Average order value and total spend
* Latest order timestamp identification
* Approval and delivery delay statistics
* Cancellation rate calculation
* Orders and revenue by state/city
* Average review score and distribution
* Inner/left join operations
* Broadcast join optimization

---

## Running Tests

### Run All Tests
```bash
pytest tests/ -v
```

### Run with Coverage Report
```bash
pytest tests/ -v --cov=src --cov-report=html --cov-report=term-missing
```

### Run Specific Test File
```bash
pytest tests/test_transforms.py -v
pytest tests/test_config.py -v
pytest tests/test_bronze_logic.py -v
pytest tests/test_silver_logic.py -v
pytest tests/test_gold_logic.py -v
```

### Run Specific Test Class
```bash
pytest tests/test_transforms.py::TestSanitizeColumnNames -v
pytest tests/test_silver_logic.py::TestOrdersTransformations -v
```

### Run Specific Test
```bash
pytest tests/test_transforms.py::TestSanitizeColumnNames::test_duplicate_column_names -v
```

### Run with Markers (if configured)
```bash
pytest -m "not slow" -v  # Skip slow tests
pytest -m integration -v  # Run only integration tests
```

---

## Test Coverage Goals

**Current Coverage Target**: 80%+

**Coverage by Module:**
* `src/utils/transforms.py` - **95%+** (core transformation logic)
* `src/01_bronze.py` - **85%+** (ingestion patterns)
* `src/02_silver.py` - **90%+** (data quality and transformations)
* `src/03_gold.py` - **85%+** (aggregation logic)
* `config/sources.json` - **100%** (configuration validation)

**View Coverage Report:**
```bash
# Generate HTML report
pytest tests/ --cov=src --cov-report=html

# Open in browser
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
start htmlcov/index.html  # Windows
```

---

## Test Structure

### Test Naming Convention
```python
class Test<Feature>:
    """Test suite for <feature> description."""
    
    def test_<specific_behavior>(self, spark):
        """Verify <expected behavior>."""
        # Arrange
        data = [...]
        
        # Act
        result = transform(data)
        
        # Assert
        assert result == expected
```

### Fixtures
```python
@pytest.fixture(scope="module")
def spark():
    """Create a local Spark session for testing."""
    return SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-tests") \
        .getOrCreate()
```

---

## Continuous Integration

Tests run automatically on every:
* **Pull Request** - All tests must pass
* **Push to main/dev** - Full test suite with coverage
* **Scheduled Runs** - Nightly regression testing

**CI/CD Configuration**: `.github/workflows/ci_cd.yml`

**Branch Protection**: Tests required before merge to main

---

## Adding New Tests

### Step 1: Create Test File
```bash
touch tests/test_new_feature.py
```

### Step 2: Follow Template
```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()

class TestNewFeature:
    """Test suite for new feature."""
    
    def test_basic_functionality(self, spark):
        """Verify basic feature works."""
        # Arrange
        data = [...]
        df = spark.createDataFrame(data, schema)
        
        # Act
        result = my_function(df)
        
        # Assert
        assert result.count() > 0
```

### Step 3: Run Locally
```bash
pytest tests/test_new_feature.py -v
```

### Step 4: Check Coverage
```bash
pytest tests/ --cov=src --cov-report=term-missing
```

---

## Best Practices

### DO:
* ✅ Test edge cases (null values, empty DataFrames, duplicates)
* ✅ Use descriptive test names that explain expected behavior
* ✅ Keep tests independent and isolated
* ✅ Use fixtures for common setup (spark session)
* ✅ Test both success and failure paths
* ✅ Verify data preservation during transformations
* ✅ Use small, focused test data

### DON'T:
* ❌ Write tests that depend on production data
* ❌ Test implementation details (test behavior, not internals)
* ❌ Create flaky tests (time-dependent, order-dependent)
* ❌ Skip error handling tests
* ❌ Write overly complex test setup
* ❌ Test third-party libraries (trust PySpark, pandas, etc.)

---

## Troubleshooting

### Tests Fail Locally But Pass in CI
**Cause**: Environment differences (Python version, dependencies)

**Fix**:
```bash
# Match CI environment
pip install -r requirements.txt
python --version  # Should be 3.10
```

### PySpark Tests Hang
**Cause**: Spark session not properly cleaned up

**Fix**: Use `scope="module"` in fixture:
```python
@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()
```

### Import Errors
**Cause**: Missing PYTHONPATH or wrong directory

**Fix**: Run from project root:
```bash
cd /path/to/databricks-dynamic-pipeline
pytest tests/
```

### Slow Test Execution
**Cause**: Too many Spark shuffle partitions

**Fix**: Configure in fixture:
```python
.config("spark.sql.shuffle.partitions", "1")
```

---

## Test Metrics

**Total Test Count**: 100+ tests

**Breakdown by File:**
* test_config.py: 25+ tests
* test_transforms.py: 15+ tests
* test_bronze_logic.py: 20+ tests
* test_silver_logic.py: 25+ tests
* test_gold_logic.py: 20+ tests

**Average Execution Time**: < 30 seconds (all tests)

**Coverage Target**: 80%+ overall, 90%+ for critical paths

---

## Next Steps

- [ ] Add integration tests that run full pipeline end-to-end
- [ ] Add performance benchmarking tests
- [ ] Add data quality validation tests
- [ ] Add schema evolution tests
- [ ] Add checkpoint recovery tests
- [ ] Add streaming Auto Loader tests

---

**Maintained by**: Panagiotis Koutridis  
**Last Updated**: 2026-08-06  
**Coverage Goal**: 80%+ (currently on track)
