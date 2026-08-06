import pytest
from pyspark.sql import SparkSession
from src.utils.transforms import sanitize_column_names

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-pyspark-local") \
        .getOrCreate()

def test_sanitize_column_names(spark):
    data = [("John", "Doe")]
    columns = ["Customer Name", "Order-ID"]
    df = spark.createDataFrame(data, columns)

    result_df = sanitize_column_names(df)

    assert "customer_name" in result_df.columns
    assert "order_id" in result_df.columns
    assert "Customer Name" not in result_df.columns