"""Tests for Silver layer transformation logic."""
import pytest
import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType


@pytest.fixture(scope="module")
def spark():
    """Create a local Spark session for testing."""
    return SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-silver-layer") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()


class TestSilverConfigParsing:
    """Test suite for silver layer configuration parsing."""

    def test_parse_silver_config_with_transform(self):
        """Verify silver config with transform parses correctly."""
        config_json = json.dumps({
            "source_name": "orders",
            "silver_transform": {
                "mode": "overwrite",
                "drop_nulls": ["order_id"]
            }
        })
        cfg = json.loads(config_json)
        assert cfg["source_name"] == "orders"
        assert "silver_transform" in cfg
        assert cfg["silver_transform"]["mode"] == "overwrite"

    def test_parse_cdc_merge_config(self):
        """Verify CDC merge config parses correctly."""
        config_json = json.dumps({
            "source_name": "orders",
            "silver_transform": {
                "mode": "cdc_merge",
                "primary_key": "order_id",
                "sequence_col": "order_purchase_timestamp"
            }
        })
        cfg = json.loads(config_json)
        transform = cfg["silver_transform"]
        assert transform["mode"] == "cdc_merge"
        assert transform["primary_key"] == "order_id"
        assert transform["sequence_col"] == "order_purchase_timestamp"

    def test_extract_drop_nulls_list(self):
        """Verify drop_nulls list extraction."""
        config_json = json.dumps({
            "source_name": "customers",
            "silver_transform": {
                "mode": "append",
                "drop_nulls": ["customer_id"]
            }
        })
        cfg = json.loads(config_json)
        drop_cols = cfg["silver_transform"].get("drop_nulls", [])
        assert isinstance(drop_cols, list)
        assert "customer_id" in drop_cols


class TestDataQualityQuarantine:
    """Test suite for data quality and quarantine logic."""

    def test_null_primary_key_filtered(self, spark):
        """Verify rows with null primary keys are filtered."""
        data = [
            ("order_1", "customer_1"),
            (None, "customer_2"),
            ("order_3", "customer_3")
        ]
        df = spark.createDataFrame(data, ["order_id", "customer_id"])
        
        # Simulate drop_nulls logic
        df_clean = df.filter("order_id IS NOT NULL")
        
        assert df_clean.count() == 2
        assert df_clean.filter("order_id IS NULL").count() == 0

    def test_quarantine_records_identified(self, spark):
        """Verify quarantine records are correctly identified."""
        data = [
            ("order_1", "customer_1"),
            (None, "customer_2"),
            ("order_3", None)
        ]
        df = spark.createDataFrame(data, ["order_id", "customer_id"])
        
        # Simulate quarantine logic for null order_id
        null_condition = "order_id IS NULL"
        df_quarantine = df.filter(null_condition).withColumn(
            "_quarantine_reason", F.lit("Missing primary key")
        )
        
        assert df_quarantine.count() == 1
        assert "_quarantine_reason" in df_quarantine.columns
        reason = df_quarantine.collect()[0]["_quarantine_reason"]
        assert reason == "Missing primary key"

    def test_multiple_null_columns_detection(self, spark):
        """Verify multiple columns can be checked for nulls."""
        data = [
            ("order_1", "customer_1", "2024-01-01"),
            (None, "customer_2", "2024-01-02"),
            ("order_3", None, "2024-01-03"),
            ("order_4", "customer_4", None)
        ]
        df = spark.createDataFrame(data, ["order_id", "customer_id", "timestamp"])
        
        # Simulate checking multiple columns
        drop_cols = ["order_id", "customer_id"]
        null_condition = " OR ".join([f"{c} IS NULL" for c in drop_cols])
        df_quarantine = df.filter(null_condition)
        
        assert df_quarantine.count() == 2  # 2 rows have nulls in order_id or customer_id


class TestOrdersTransformations:
    """Test suite for orders-specific transformations."""

    def test_timestamp_parsing(self, spark):
        """Verify timestamp columns are parsed correctly."""
        data = [(
            "order_1",
            "2024-01-15 10:30:00",
            "2024-01-15 11:00:00"
        )]
        df = spark.createDataFrame(data, [
            "order_id",
            "order_purchase_timestamp",
            "order_approved_at"
        ])
        
        df_transformed = df \
            .withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp")) \
            .withColumn("order_approved_at", F.to_timestamp("order_approved_at"))
        
        assert df_transformed.schema["order_purchase_timestamp"].dataType == TimestampType()
        assert df_transformed.schema["order_approved_at"].dataType == TimestampType()

    def test_approval_delay_calculation(self, spark):
        """Verify approval delay calculation in hours."""
        data = [(
            "order_1",
            "2024-01-15 10:00:00",
            "2024-01-15 12:30:00"  # 2.5 hours later
        )]
        df = spark.createDataFrame(data, [
            "order_id",
            "order_purchase_timestamp",
            "order_approved_at"
        ])
        
        df_transformed = df \
            .withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp")) \
            .withColumn("order_approved_at", F.to_timestamp("order_approved_at")) \
            .withColumn(
                "approval_delay_hours",
                F.round(
                    (F.unix_timestamp("order_approved_at") - F.unix_timestamp("order_purchase_timestamp")) / 3600,
                    2
                )
            )
        
        result = df_transformed.collect()[0]
        assert result["approval_delay_hours"] == 2.5

    def test_delivery_delay_calculation(self, spark):
        """Verify delivery delay calculation in days."""
        data = [(
            "order_1",
            "2024-01-20",  # Actual delivery
            "2024-01-15"   # Estimated delivery
        )]
        df = spark.createDataFrame(data, [
            "order_id",
            "order_delivered_customer_date",
            "order_estimated_delivery_date"
        ])
        
        df_transformed = df \
            .withColumn("order_delivered_customer_date", F.to_timestamp("order_delivered_customer_date")) \
            .withColumn("order_estimated_delivery_date", F.to_timestamp("order_estimated_delivery_date")) \
            .withColumn(
                "delivery_delay_days",
                F.datediff("order_delivered_customer_date", "order_estimated_delivery_date")
            )
        
        result = df_transformed.collect()[0]
        assert result["delivery_delay_days"] == 5  # 5 days late

    def test_negative_delivery_delay_early(self, spark):
        """Verify negative delay when delivered early."""
        data = [(
            "order_1",
            "2024-01-10",  # Actual (early)
            "2024-01-15"   # Estimated
        )]
        df = spark.createDataFrame(data, [
            "order_id",
            "order_delivered_customer_date",
            "order_estimated_delivery_date"
        ])
        
        df_transformed = df \
            .withColumn("order_delivered_customer_date", F.to_timestamp("order_delivered_customer_date")) \
            .withColumn("order_estimated_delivery_date", F.to_timestamp("order_estimated_delivery_date")) \
            .withColumn(
                "delivery_delay_days",
                F.datediff("order_delivered_customer_date", "order_estimated_delivery_date")
            )
        
        result = df_transformed.collect()[0]
        assert result["delivery_delay_days"] == -5  # 5 days early


class TestReviewsTransformations:
    """Test suite for reviews-specific transformations."""

    def test_review_score_casting(self, spark):
        """Verify review_score is cast to integer."""
        data = [("review_1", "5"), ("review_2", "3")]
        df = spark.createDataFrame(data, ["review_id", "review_score"])
        
        df_transformed = df.withColumn("review_score", F.col("review_score").cast("int"))
        
        assert df_transformed.schema["review_score"].dataType == IntegerType()
        scores = [r["review_score"] for r in df_transformed.collect()]
        assert scores == [5, 3]

    def test_review_length_calculation(self, spark):
        """Verify review comment length calculation."""
        data = [
            ("review_1", "Great product!"),
            ("review_2", "Not satisfied")
        ]
        df = spark.createDataFrame(data, ["review_id", "review_comment_message"])
        
        df_transformed = df.withColumn(
            "review_length",
            F.length(F.col("review_comment_message"))
        )
        
        results = df_transformed.collect()
        assert results[0]["review_length"] == len("Great product!")
        assert results[1]["review_length"] == len("Not satisfied")

    def test_review_timestamp_parsing(self, spark):
        """Verify review timestamp columns are parsed."""
        data = [(
            "review_1",
            "2024-01-15 10:30:00",
            "2024-01-16 12:00:00"
        )]
        df = spark.createDataFrame(data, [
            "review_id",
            "review_creation_date",
            "review_answer_timestamp"
        ])
        
        df_transformed = df \
            .withColumn("review_creation_date", F.to_timestamp("review_creation_date")) \
            .withColumn("review_answer_timestamp", F.to_timestamp("review_answer_timestamp"))
        
        assert df_transformed.schema["review_creation_date"].dataType == TimestampType()
        assert df_transformed.schema["review_answer_timestamp"].dataType == TimestampType()


class TestCustomersTransformations:
    """Test suite for customers-specific transformations."""

    def test_city_standardization(self, spark):
        """Verify customer city is standardized to uppercase."""
        data = [
            ("customer_1", "  sao paulo  "),
            ("customer_2", "Rio de Janeiro")
        ]
        df = spark.createDataFrame(data, ["customer_id", "customer_city"])
        
        df_transformed = df.withColumn(
            "customer_city",
            F.upper(F.trim("customer_city"))
        )
        
        results = df_transformed.collect()
        assert results[0]["customer_city"] == "SAO PAULO"
        assert results[1]["customer_city"] == "RIO DE JANEIRO"

    def test_state_standardization(self, spark):
        """Verify customer state is standardized to uppercase."""
        data = [
            ("customer_1", "  sp  "),
            ("customer_2", "rj")
        ]
        df = spark.createDataFrame(data, ["customer_id", "customer_state"])
        
        df_transformed = df.withColumn(
            "customer_state",
            F.upper(F.trim("customer_state"))
        )
        
        results = df_transformed.collect()
        assert results[0]["customer_state"] == "SP"
        assert results[1]["customer_state"] == "RJ"


class TestCDCMergeLogic:
    """Test suite for CDC merge deduplication logic."""

    def test_deduplication_keeps_latest(self, spark):
        """Verify CDC merge keeps latest record per primary key."""
        data = [
            ("order_1", "2024-01-15 10:00:00", "status_1"),
            ("order_1", "2024-01-15 11:00:00", "status_2"),  # Latest
            ("order_2", "2024-01-15 09:00:00", "status_3")
        ]
        df = spark.createDataFrame(data, ["order_id", "timestamp", "status"])
        
        df = df.withColumn("timestamp", F.to_timestamp("timestamp"))
        
        # Simulate CDC deduplication
        window_spec = Window.partitionBy("order_id").orderBy(F.col("timestamp").desc())
        deduped_df = df.withColumn("_rn", F.row_number().over(window_spec)) \
            .filter("_rn = 1") \
            .drop("_rn")
        
        assert deduped_df.count() == 2
        order_1_status = deduped_df.filter("order_id = 'order_1'").collect()[0]["status"]
        assert order_1_status == "status_2"  # Latest status kept

    def test_deduplication_with_multiple_updates(self, spark):
        """Verify deduplication handles multiple updates correctly."""
        data = [
            ("order_1", "2024-01-15 10:00:00", 100.0),
            ("order_1", "2024-01-15 11:00:00", 110.0),
            ("order_1", "2024-01-15 12:00:00", 120.0),  # Latest
            ("order_2", "2024-01-15 09:00:00", 200.0)
        ]
        df = spark.createDataFrame(data, ["order_id", "timestamp", "amount"])
        
        df = df.withColumn("timestamp", F.to_timestamp("timestamp"))
        
        window_spec = Window.partitionBy("order_id").orderBy(F.col("timestamp").desc())
        deduped_df = df.withColumn("_rn", F.row_number().over(window_spec)) \
            .filter("_rn = 1") \
            .drop("_rn")
        
        assert deduped_df.count() == 2
        order_1_amount = deduped_df.filter("order_id = 'order_1'").collect()[0]["amount"]
        assert order_1_amount == 120.0