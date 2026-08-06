from pyspark.sql import functions as F

def sanitize_column_names(df):
    """Standardizes column naming conventions for optimal Delta parquet storage."""
    for col in df.columns:
        clean_col = col.strip().lower().replace(" ", "_").replace("-", "_")
        df = df.withColumnRenamed(col, clean_col)
    return df

def join_ecom_domain(spark):
    """Broadcast-optimized multi-source Gold domain join."""
    df_orders = spark.table("main.silver.olist_orders")
    df_cust = spark.table("main.silver.olist_customers")
    df_rev = spark.table("main.silver.olist_reviews")

    return (
        df_orders
        .join(F.broadcast(df_cust), on="customer_id", how="inner")
        .join(df_rev, on="order_id", how="left")
        .groupBy("customer_state", "customer_city")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.avg("review_score").alias("avg_satisfaction_score"),
            F.max("order_purchase_timestamp").alias("latest_order_timestamp")
        )
    )

custom_transform_registry = {
    "join_ecom_domain": join_ecom_domain
}