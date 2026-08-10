"""Tests for Gold layer aggregation logic."""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="module")
def spark():
    """Create a local Spark session for testing."""
    return SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-gold-layer") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()


class TestCustomerOrderSummary:
    """Test suite for customer order summary aggregations."""

    def test_customer_order_count(self, spark):
        """Verify customer order count aggregation."""
        data = [
            ("customer_1", "order_1"),
            ("customer_1", "order_2"),
            ("customer_2", "order_3")
        ]
        df = spark.createDataFrame(data, ["customer_id", "order_id"])

        result_df = df.groupBy("customer_id").agg(
            F.count("order_id").alias("total_orders")
        )

        results = result_df.collect()
        customer_1_orders = [r for r in results if r["customer_id"] == "customer_1"][0]
        assert customer_1_orders["total_orders"] == 2

    def test_distinct_order_count(self, spark):
        """Verify distinct order counting handles duplicates."""
        data = [
            ("customer_1", "order_1"),
            ("customer_1", "order_1"),  # Duplicate
            ("customer_1", "order_2")
        ]
        df = spark.createDataFrame(data, ["customer_id", "order_id"])

        result_df = df.groupBy("customer_id").agg(
            F.countDistinct("order_id").alias("total_orders")
        )

        result = result_df.collect()[0]
        assert result["total_orders"] == 2  # Only 2 distinct orders

    def test_average_order_value(self, spark):
        """Verify average order value calculation."""
        data = [
            ("customer_1", "order_1", 100.0),
            ("customer_1", "order_2", 200.0),
            ("customer_2", "order_3", 150.0)
        ]
        df = spark.createDataFrame(data, ["customer_id", "order_id", "order_value"])

        result_df = df.groupBy("customer_id").agg(
            F.avg("order_value").alias("avg_order_value")
        )

        customer_1 = result_df.filter("customer_id = 'customer_1'").collect()[0]
        assert customer_1["avg_order_value"] == 150.0  # (100 + 200) / 2

    def test_total_spend_calculation(self, spark):
        """Verify total customer spend aggregation."""
        data = [
            ("customer_1", 100.0),
            ("customer_1", 200.0),
            ("customer_2", 150.0)
        ]
        df = spark.createDataFrame(data, ["customer_id", "order_value"])

        result_df = df.groupBy("customer_id").agg(
            F.sum("order_value").alias("total_spend")
        )

        customer_1 = result_df.filter("customer_id = 'customer_1'").collect()[0]
        assert customer_1["total_spend"] == 300.0

    def test_latest_order_timestamp(self, spark):
        """Verify latest order timestamp identification."""
        data = [
            ("customer_1", "2024-01-15 10:00:00"),
            ("customer_1", "2024-01-20 15:30:00"),  # Latest
            ("customer_2", "2024-01-10 09:00:00")
        ]
        df = spark.createDataFrame(data, ["customer_id", "order_timestamp"])
        df = df.withColumn("order_timestamp", F.to_timestamp("order_timestamp"))

        result_df = df.groupBy("customer_id").agg(
            F.max("order_timestamp").alias("latest_order")
        )

        customer_1 = result_df.filter("customer_id = 'customer_1'").collect()[0]
        assert str(customer_1["latest_order"]) == "2024-01-20 15:30:00"


class TestOrderPerformanceMetrics:
    """Test suite for order performance metric calculations."""

    def test_approval_delay_average(self, spark):
        """Verify average approval delay calculation."""
        data = [
            ("order_1", 2.5),
            ("order_2", 1.0),
            ("order_3", 3.5)
        ]
        df = spark.createDataFrame(data, ["order_id", "approval_delay_hours"])

        avg_delay = df.agg(F.avg("approval_delay_hours").alias("avg_delay")).collect()[0]["avg_delay"]

        assert round(avg_delay, 2) == 2.33  # (2.5 + 1.0 + 3.5) / 3

    def test_delivery_delay_statistics(self, spark):
        """Verify delivery delay min/max/avg calculations."""
        data = [
            ("order_1", 5),   # 5 days late
            ("order_2", -2),  # 2 days early
            ("order_3", 0),   # On time
            ("order_4", 10)   # 10 days late
        ]
        df = spark.createDataFrame(data, ["order_id", "delivery_delay_days"])

        result = df.agg(
            F.min("delivery_delay_days").alias("min_delay"),
            F.max("delivery_delay_days").alias("max_delay"),
            F.avg("delivery_delay_days").alias("avg_delay")
        ).collect()[0]

        assert result["min_delay"] == -2
        assert result["max_delay"] == 10
        assert result["avg_delay"] == 3.25  # (5 + (-2) + 0 + 10) / 4

    def test_cancellation_rate_calculation(self, spark):
        """Verify order cancellation rate calculation."""
        data = [
            ("order_1", "delivered"),
            ("order_2", "canceled"),
            ("order_3", "delivered"),
            ("order_4", "canceled")
        ]
        df = spark.createDataFrame(data, ["order_id", "order_status"])

        result = df.agg(
            F.count("*").alias("total_orders"),
            F.sum(F.when(F.col("order_status") == "canceled", 1).otherwise(0)).alias("canceled_orders")
        ).collect()[0]

        cancellation_rate = (result["canceled_orders"] / result["total_orders"]) * 100
        assert cancellation_rate == 50.0  # 2 out of 4 canceled


class TestGeographicAnalysis:
    """Test suite for geographic analysis aggregations."""

    def test_orders_by_state(self, spark):
        """Verify order count aggregation by state."""
        data = [
            ("SP", "order_1"),
            ("SP", "order_2"),
            ("RJ", "order_3"),
            ("SP", "order_4")
        ]
        df = spark.createDataFrame(data, ["customer_state", "order_id"])

        result_df = df.groupBy("customer_state").agg(
            F.count("order_id").alias("total_orders")
        ).orderBy(F.desc("total_orders"))

        results = result_df.collect()
        assert results[0]["customer_state"] == "SP"
        assert results[0]["total_orders"] == 3

    def test_orders_by_city_and_state(self, spark):
        """Verify order aggregation by city and state."""
        data = [
            ("SP", "SAO PAULO", "order_1"),
            ("SP", "SAO PAULO", "order_2"),
            ("SP", "CAMPINAS", "order_3"),
            ("RJ", "RIO DE JANEIRO", "order_4")
        ]
        df = spark.createDataFrame(data, ["customer_state", "customer_city", "order_id"])

        result_df = df.groupBy("customer_state", "customer_city").agg(
            F.count("order_id").alias("total_orders")
        )

        sao_paulo = result_df.filter(
            "customer_state = 'SP' AND customer_city = 'SAO PAULO'"
        ).collect()[0]
        assert sao_paulo["total_orders"] == 2

    def test_revenue_by_region(self, spark):
        """Verify revenue aggregation by geographic region."""
        data = [
            ("SP", 100.0),
            ("SP", 200.0),
            ("RJ", 150.0)
        ]
        df = spark.createDataFrame(data, ["customer_state", "order_value"])

        result_df = df.groupBy("customer_state").agg(
            F.sum("order_value").alias("total_revenue")
        ).orderBy(F.desc("total_revenue"))

        top_state = result_df.collect()[0]
        assert top_state["customer_state"] == "SP"
        assert top_state["total_revenue"] == 300.0


class TestReviewAnalytics:
    """Test suite for review analytics aggregations."""

    def test_average_review_score(self, spark):
        """Verify average review score calculation."""
        data = [
            ("product_1", 5),
            ("product_1", 4),
            ("product_1", 5)
        ]
        df = spark.createDataFrame(data, ["product_id", "review_score"])

        result = df.groupBy("product_id").agg(
            F.avg("review_score").alias("avg_score")
        ).collect()[0]

        assert round(result["avg_score"], 2) == 4.67  # (5 + 4 + 5) / 3

    def test_review_score_distribution(self, spark):
        """Verify review score distribution calculation."""
        data = [
            ("review_1", 5),
            ("review_2", 5),
            ("review_3", 4),
            ("review_4", 3),
            ("review_5", 5)
        ]
        df = spark.createDataFrame(data, ["review_id", "review_score"])

        result_df = df.groupBy("review_score").agg(
            F.count("*").alias("count")
        ).orderBy("review_score")

        results = {r["review_score"]: r["count"] for r in result_df.collect()}
        assert results[5] == 3
        assert results[4] == 1
        assert results[3] == 1

    def test_average_review_length(self, spark):
        """Verify average review comment length calculation."""
        data = [
            ("review_1", "Great!"),       # 6 chars
            ("review_2", "Not good"),     # 8 chars
            ("review_3", "Excellent")     # 9 chars
        ]
        df = spark.createDataFrame(data, ["review_id", "review_comment"])

        df_with_length = df.withColumn("review_length", F.length("review_comment"))
        avg_length = df_with_length.agg(
            F.avg("review_length").alias("avg_length")
        ).collect()[0]["avg_length"]

        assert round(avg_length, 2) == 7.67  # (6 + 8 + 9) / 3


class TestJoinOperations:
    """Test suite for multi-table join operations."""

    def test_inner_join_customers_orders(self, spark):
        """Verify inner join between customers and orders."""
        customers = [
            ("customer_1", "SP"),
            ("customer_2", "RJ")
        ]
        orders = [
            ("order_1", "customer_1"),
            ("order_2", "customer_1"),
            ("order_3", "customer_2")
        ]

        df_customers = spark.createDataFrame(customers, ["customer_id", "customer_state"])
        df_orders = spark.createDataFrame(orders, ["order_id", "customer_id"])

        joined_df = df_orders.join(df_customers, on="customer_id", how="inner")

        assert joined_df.count() == 3
        assert "customer_state" in joined_df.columns

    def test_left_join_orders_reviews(self, spark):
        """Verify left join between orders and reviews."""
        orders = [
            ("order_1",),
            ("order_2",),
            ("order_3",)
        ]
        reviews = [
            ("order_1", 5),
            ("order_2", 4)
            # order_3 has no review
        ]

        df_orders = spark.createDataFrame(orders, ["order_id"])
        df_reviews = spark.createDataFrame(reviews, ["order_id", "review_score"])

        joined_df = df_orders.join(df_reviews, on="order_id", how="left")

        assert joined_df.count() == 3
        # order_3 should have null review_score
        order_3 = joined_df.filter("order_id = 'order_3'").collect()[0]
        assert order_3["review_score"] is None

    def test_broadcast_join_optimization(self, spark):
        """Verify broadcast hint can be applied for small tables."""
        small_table = [("customer_1", "SP")]
        large_table = [("order_1", "customer_1"), ("order_2", "customer_1")]

        df_small = spark.createDataFrame(small_table, ["customer_id", "customer_state"])
        df_large = spark.createDataFrame(large_table, ["order_id", "customer_id"])

        # Apply broadcast hint
        joined_df = df_large.join(F.broadcast(df_small), on="customer_id", how="inner")

        assert joined_df.count() == 2
        assert "customer_state" in joined_df.columns
