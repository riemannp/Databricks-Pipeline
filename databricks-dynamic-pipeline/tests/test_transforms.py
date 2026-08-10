import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.utils.transforms import sanitize_column_names, custom_transform_registry


@pytest.fixture(scope="module")
def spark():
    """Create a local Spark session for testing."""
    return SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-pyspark-local") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()


class TestSanitizeColumnNames:
    """Test suite for column name sanitization."""

    def test_basic_sanitization(self, spark):
        """Test basic column name cleaning: spaces and hyphens."""
        data = [("John", "Doe")]
        columns = ["Customer Name", "Order-ID"]
        df = spark.createDataFrame(data, columns)

        result_df = sanitize_column_names(df)

        assert "customer_name" in result_df.columns
        assert "order_id" in result_df.columns
        assert "Customer Name" not in result_df.columns
        assert "Order-ID" not in result_df.columns

    def test_duplicate_column_names(self, spark):
        """Test handling of duplicate column names after sanitization."""
        data = [(1, 2, 3)]
        columns = ["Order ID", "Order-ID", "order_id"]
        df = spark.createDataFrame(data, columns)

        result_df = sanitize_column_names(df)

        # First occurrence keeps original name, duplicates get suffix
        assert "order_id" in result_df.columns
        assert "order_id_1" in result_df.columns
        assert "order_id_2" in result_df.columns

    def test_multiple_duplicates(self, spark):
        """Test handling multiple duplicate columns."""
        data = [(1, 2, 3, 4)]
        columns = ["Customer Name", "customer-name", "CUSTOMER NAME", "customer_name"]
        df = spark.createDataFrame(data, columns)

        result_df = sanitize_column_names(df)

        assert "customer_name" in result_df.columns
        assert "customer_name_1" in result_df.columns
        assert "customer_name_2" in result_df.columns
        assert "customer_name_3" in result_df.columns

    def test_special_characters(self, spark):
        """Test column names with special characters."""
        data = [(1, 2)]
        columns = ["Order#ID", "Customer@Email"]
        df = spark.createDataFrame(data, columns)

        result_df = sanitize_column_names(df)

        # Only spaces and hyphens are replaced; other chars remain
        assert "order#id" in result_df.columns
        assert "customer@email" in result_df.columns

    def test_whitespace_handling(self, spark):
        """Test trimming of leading/trailing whitespace."""
        data = [(1, 2)]
        columns = ["  Order ID  ", "  Customer Name  "]
        df = spark.createDataFrame(data, columns)

        result_df = sanitize_column_names(df)

        assert "order_id" in result_df.columns
        assert "customer_name" in result_df.columns

    def test_mixed_case_conversion(self, spark):
        """Test lowercase conversion."""
        data = [(1, 2)]
        columns = ["UPPER_CASE", "MixedCase", "lower_case"]
        df = spark.createDataFrame(data, columns)

        result_df = sanitize_column_names(df)

        assert "upper_case" in result_df.columns
        assert "mixedcase" in result_df.columns
        assert "lower_case" in result_df.columns

    def test_empty_dataframe(self, spark):
        """Test sanitization on empty DataFrame."""
        schema = "Order ID STRING, Customer-Name STRING"
        df = spark.createDataFrame([], schema)

        result_df = sanitize_column_names(df)

        assert "order_id" in result_df.columns
        assert "customer_name" in result_df.columns
        assert result_df.count() == 0

    def test_data_preservation(self, spark):
        """Ensure data is not lost during column renaming."""
        data = [("John", 123), ("Jane", 456)]
        columns = ["Customer Name", "Order-ID"]
        df = spark.createDataFrame(data, columns)

        result_df = sanitize_column_names(df)

        assert result_df.count() == 2
        first_row = result_df.collect()[0]
        assert first_row["customer_name"] == "John"
        assert first_row["order_id"] == 123


class TestCustomTransformRegistry:
    """Test suite for custom transform registry."""

    def test_registry_contains_join_ecom_domain(self):
        """Verify join_ecom_domain is registered."""
        assert "join_ecom_domain" in custom_transform_registry
        assert callable(custom_transform_registry["join_ecom_domain"])

    def test_registry_is_dict(self):
        """Verify registry is a dictionary."""
        assert isinstance(custom_transform_registry, dict)


class TestJoinEcomDomain:
    """Test suite for join_ecom_domain transform (mock-based)."""

    def test_function_callable(self):
        """Verify function is callable and accepts spark parameter."""
        from src.utils.transforms import join_ecom_domain
        import inspect

        assert callable(join_ecom_domain)
        sig = inspect.signature(join_ecom_domain)
        assert "spark" in sig.parameters

    # Note: Full integration test would require mock tables or test data setup
    # For now, we verify the function exists and has correct signature