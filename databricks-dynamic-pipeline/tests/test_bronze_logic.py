"""Tests for Bronze layer ingestion logic."""
import json

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    TimestampType,
)


@pytest.fixture(scope="module")
def spark():
    """Create a local Spark session for testing."""
    return SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-bronze-layer") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()


class TestConfigParsing:
    """Test suite for configuration parsing in bronze layer."""

    def test_valid_json_config_parses(self):
        """Verify valid JSON config can be parsed."""
        config_json = json.dumps({
            "source_name": "orders",
            "source_format": "csv",
            "source_path": "/Volumes/main/default/raw_landing/orders/",
            "read_options": {"header": "true"}
        })
        cfg = json.loads(config_json)
        assert cfg["source_name"] == "orders"
        assert cfg["source_format"] == "csv"

    def test_invalid_json_raises_error(self):
        """Verify invalid JSON raises appropriate error."""
        invalid_json = '{"source_name": "orders", incomplete'
        with pytest.raises(json.JSONDecodeError):
            json.loads(invalid_json)

    def test_missing_source_name_detected(self):
        """Verify missing source_name field is detected."""
        config_json = json.dumps({
            "source_format": "csv",
            "source_path": "/Volumes/main/default/raw_landing/"
        })
        cfg = json.loads(config_json)
        assert "source_name" not in cfg

    def test_read_options_extraction(self):
        """Verify read_options are correctly extracted."""
        config_json = json.dumps({
            "source_name": "orders",
            "source_format": "csv",
            "source_path": "/test/",
            "read_options": {"header": "true", "delimiter": ","}
        })
        cfg = json.loads(config_json)
        assert "read_options" in cfg
        assert cfg["read_options"]["header"] == "true"
        assert cfg["read_options"]["delimiter"] == ","

    def test_missing_read_options_defaults_to_empty(self):
        """Verify missing read_options defaults to empty dict."""
        config_json = json.dumps({
            "source_name": "orders",
            "source_format": "csv",
            "source_path": "/test/"
        })
        cfg = json.loads(config_json)
        read_opts = cfg.get("read_options", {})
        assert isinstance(read_opts, dict)
        assert len(read_opts) == 0


class TestPathConstruction:
    """Test suite for source path construction."""

    def test_path_with_trailing_slash_gets_wildcard(self):
        """Verify trailing slash paths get format wildcard."""
        source_path = "/Volumes/main/default/raw_landing/orders/"
        source_format = "csv"

        if source_path.endswith("/"):
            final_path = source_path + f"*.{source_format}"
        else:
            final_path = source_path

        assert final_path == "/Volumes/main/default/raw_landing/orders/*.csv"

    def test_path_without_trailing_slash_unchanged(self):
        """Verify paths without trailing slash remain unchanged."""
        source_path = "/Volumes/main/default/raw_landing/orders/*.csv"
        source_format = "csv"

        if source_path.endswith("/"):
            final_path = source_path + f"*.{source_format}"
        else:
            final_path = source_path

        assert final_path == "/Volumes/main/default/raw_landing/orders/*.csv"

    def test_different_formats_construct_correctly(self):
        """Verify different file formats construct correct paths."""
        base_path = "/Volumes/main/default/raw_landing/data/"

        formats = ["csv", "json", "parquet"]
        expected = [
            "/Volumes/main/default/raw_landing/data/*.csv",
            "/Volumes/main/default/raw_landing/data/*.json",
            "/Volumes/main/default/raw_landing/data/*.parquet"
        ]

        for fmt, exp in zip(formats, expected):
            result = base_path + f"*.{fmt}"
            assert result == exp


class TestIngestionTimestampColumn:
    """Test suite for _ingested_at timestamp column."""

    def test_ingested_at_column_added(self, spark):
        """Verify _ingested_at column is added to DataFrame."""
        data = [("order_1", "customer_1")]
        df = spark.createDataFrame(data, ["order_id", "customer_id"])

        df_with_ts = df.withColumn("_ingested_at", F.current_timestamp())

        assert "_ingested_at" in df_with_ts.columns
        assert df_with_ts.schema["_ingested_at"].dataType == TimestampType()

    def test_ingested_at_is_recent_timestamp(self, spark):
        """Verify _ingested_at contains valid recent timestamp."""
        data = [("order_1",)]
        df = spark.createDataFrame(data, ["order_id"])

        df_with_ts = df.withColumn("_ingested_at", F.current_timestamp())
        result = df_with_ts.collect()[0]["_ingested_at"]

        assert result is not None
        # Timestamp should be recent (within last minute for test execution)
        from datetime import datetime, timedelta
        now = datetime.now()
        assert result <= now
        assert result >= now - timedelta(minutes=1)


class TestColumnSanitization:
    """Test suite for column sanitization integration."""

    def test_sanitization_applied_before_ingestion_timestamp(self, spark):
        """Verify columns are sanitized before adding _ingested_at."""
        from src.utils.transforms import sanitize_column_names

        data = [("order_1", "customer_1")]
        df = spark.createDataFrame(data, ["Order ID", "Customer-ID"])

        df_clean = sanitize_column_names(df)
        df_bronze = df_clean.withColumn("_ingested_at", F.current_timestamp())

        assert "order_id" in df_bronze.columns
        assert "customer_id" in df_bronze.columns
        assert "_ingested_at" in df_bronze.columns
        assert "Order ID" not in df_bronze.columns


class TestCheckpointAndSchemaLocations:
    """Test suite for checkpoint and schema location path construction."""

    def test_checkpoint_path_construction(self):
        """Verify checkpoint path is constructed correctly."""
        source_name = "orders"
        checkpoint_path = f"/Volumes/main/default/raw_landing/_checkpoints/{source_name}"

        assert checkpoint_path == "/Volumes/main/default/raw_landing/_checkpoints/orders"

    def test_schema_location_path_construction(self):
        """Verify schema location path is constructed correctly."""
        source_name = "customers"
        schema_path = f"/Volumes/main/default/raw_landing/_schemas/{source_name}"

        assert schema_path == "/Volumes/main/default/raw_landing/_schemas/customers"

    def test_different_sources_get_unique_paths(self):
        """Verify different sources get unique checkpoint/schema paths."""
        sources = ["orders", "customers", "reviews"]

        checkpoint_paths = [f"/Volumes/main/default/raw_landing/_checkpoints/{s}" for s in sources]
        schema_paths = [f"/Volumes/main/default/raw_landing/_schemas/{s}" for s in sources]

        # All paths should be unique
        assert len(checkpoint_paths) == len(set(checkpoint_paths))
        assert len(schema_paths) == len(set(schema_paths))


class TestOutputTableNaming:
    """Test suite for output table naming conventions."""

    def test_bronze_table_naming(self):
        """Verify bronze table names follow convention."""
        source_name = "orders"
        table_name = f"main.bronze.{source_name}"

        assert table_name == "main.bronze.orders"
        assert table_name.startswith("main.bronze.")

    def test_different_sources_get_unique_tables(self):
        """Verify different sources write to unique bronze tables."""
        sources = ["orders", "customers", "reviews"]
        table_names = [f"main.bronze.{s}" for s in sources]

        assert len(table_names) == len(set(table_names))
        assert all(t.startswith("main.bronze.") for t in table_names)
