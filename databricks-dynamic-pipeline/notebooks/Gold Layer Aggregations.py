# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Gold Layer Aggregations
# MAGIC %md
# MAGIC # Gold Layer Aggregations
# MAGIC
# MAGIC This notebook creates business-ready analytics tables from silver data:
# MAGIC - **Customer Order Summary**: Lifetime order metrics per customer
# MAGIC - **Order Performance**: Delivery and approval time analytics
# MAGIC - **Review Analytics**: Score distributions and sentiment metrics
# MAGIC - **Geographic Analysis**: Orders and reviews by state/city

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Customer Order Summary
# Aggregate customer order metrics
df_orders = spark.table("main.silver.orders")
df_reviews = spark.table("main.silver.reviews")

# Join orders with reviews to get review scores
df_orders_reviews = df_orders.join(
    df_reviews.select("order_id", "review_score"),
    "order_id",
    "left"
)

# Calculate customer-level metrics
df_customer_summary = df_orders_reviews.groupBy("customer_id").agg(
    F.count("order_id").alias("total_orders"),
    F.countDistinct("order_id").alias("unique_orders"),
    F.avg("review_score").alias("avg_review_score"),
    F.sum(F.when(F.col("order_status") == "delivered", 1).otherwise(0)).alias("delivered_orders"),
    F.sum(F.when(F.col("order_status") == "canceled", 1).otherwise(0)).alias("canceled_orders"),
    F.avg("approval_delay_hours").alias("avg_approval_delay_hours"),
    F.avg("delivery_delay_days").alias("avg_delivery_delay_days"),
    F.min("order_purchase_timestamp").alias("first_order_date"),
    F.max("order_purchase_timestamp").alias("last_order_date")
)

# Add customer lifetime days
df_customer_summary = df_customer_summary.withColumn(
    "customer_lifetime_days",
    F.datediff("last_order_date", "first_order_date")
)

# Write to gold layer
df_customer_summary.write.format("delta").mode("overwrite").saveAsTable("main.gold.customer_order_summary")

print(f"✅ Customer Order Summary created: {df_customer_summary.count()} customers")

# COMMAND ----------

# DBTITLE 1,Order Performance Metrics
# Aggregate order performance by status and time period
df_orders = spark.table("main.silver.orders")

# Add time dimensions
df_orders_enriched = df_orders.withColumn(
    "order_year", F.year("order_purchase_timestamp")
).withColumn(
    "order_month", F.month("order_purchase_timestamp")
).withColumn(
    "order_quarter", F.quarter("order_purchase_timestamp")
)

# Calculate performance metrics by time and status
df_order_performance = df_orders_enriched.groupBy(
    "order_year", "order_quarter", "order_month", "order_status"
).agg(
    F.count("order_id").alias("order_count"),
    F.avg("approval_delay_hours").alias("avg_approval_delay_hours"),
    F.avg("delivery_delay_days").alias("avg_delivery_delay_days"),
    F.percentile_approx("approval_delay_hours", 0.5).alias("median_approval_delay_hours"),
    F.percentile_approx("delivery_delay_days", 0.5).alias("median_delivery_delay_days"),
    F.min("approval_delay_hours").alias("min_approval_delay_hours"),
    F.max("approval_delay_hours").alias("max_approval_delay_hours")
)

# Write to gold layer
df_order_performance.write.format("delta").mode("overwrite").saveAsTable("main.gold.order_performance_metrics")

print(f"✅ Order Performance Metrics created: {df_order_performance.count()} records")

# COMMAND ----------

# DBTITLE 1,Review Analytics
df_reviews = spark.table("main.silver.reviews")

df_reviews_enriched = df_reviews.withColumn(
    "review_year", F.year("review_creation_date")
).withColumn(
    "review_month", F.month("review_creation_date")
).withColumn(
    "review_sentiment", F.when(F.col("review_score") >= 4, "positive")
                          .when(F.col("review_score") >= 3, "neutral")
                          .otherwise("negative")
)

df_review_analytics = df_reviews_enriched.groupBy(
    "review_year", "review_month", "review_sentiment"
).agg(
    F.count("review_id").alias("review_count"),
    F.avg("review_score").alias("avg_review_score"),
    F.avg("review_length").alias("avg_review_length"),
    F.sum(F.when(F.col("review_comment_message").isNotNull(), 1).otherwise(0)).alias("reviews_with_comments")
)

window_spec = Window.partitionBy("review_year", "review_month")
df_review_analytics = df_review_analytics.withColumn(
    "total_reviews_in_period",
    F.sum("review_count").over(window_spec)
).withColumn(
    "sentiment_percentage",
    F.round((F.col("review_count") / F.col("total_reviews_in_period")) * 100, 2)
)

df_review_analytics.write.format("delta").mode("overwrite").saveAsTable("main.gold.review_analytics")

print(f"Review Analytics created: {df_review_analytics.count()} records")

# COMMAND ----------

# DBTITLE 1,Geographic Analysis
df_customers = spark.table("main.silver.customers")
df_orders = spark.table("main.silver.orders")
df_reviews = spark.table("main.silver.reviews")

df_orders_geo = df_orders.join(
    df_customers.select("customer_id", "customer_city", "customer_state"),
    "customer_id",
    "inner"
)

df_orders_geo_reviews = df_orders_geo.join(
    df_reviews.select("order_id", "review_score"),
    "order_id",
    "left"
)

df_geographic_analysis = df_orders_geo_reviews.groupBy(
    "customer_state", "customer_city"
).agg(
    F.count("order_id").alias("total_orders"),
    F.countDistinct("customer_id").alias("unique_customers"),
    F.avg("review_score").alias("avg_review_score"),
    F.avg("approval_delay_hours").alias("avg_approval_delay_hours"),
    F.avg("delivery_delay_days").alias("avg_delivery_delay_days"),
    F.sum(F.when(F.col("order_status") == "delivered", 1).otherwise(0)).alias("delivered_orders"),
    F.sum(F.when(F.col("order_status") == "canceled", 1).otherwise(0)).alias("canceled_orders")
).withColumn(
    "delivery_rate",
    F.round((F.col("delivered_orders") / F.col("total_orders")) * 100, 2)
).withColumn(
    "cancellation_rate",
    F.round((F.col("canceled_orders") / F.col("total_orders")) * 100, 2)
)

df_geographic_analysis.write.format("delta").mode("overwrite").saveAsTable("main.gold.geographic_analysis")

print(f"Geographic Analysis created: {df_geographic_analysis.count()} locations")

# COMMAND ----------

# DBTITLE 1,Verify Gold Tables
# MAGIC %sql
# MAGIC -- Verify all gold tables
# MAGIC SELECT 'customer_order_summary' as table_name, COUNT(*) as row_count FROM main.gold.customer_order_summary
# MAGIC UNION ALL
# MAGIC SELECT 'order_performance_metrics' as table_name, COUNT(*) as row_count FROM main.gold.order_performance_metrics
# MAGIC UNION ALL
# MAGIC SELECT 'review_analytics' as table_name, COUNT(*) as row_count FROM main.gold.review_analytics
# MAGIC UNION ALL
# MAGIC SELECT 'geographic_analysis' as table_name, COUNT(*) as row_count FROM main.gold.geographic_analysis

# COMMAND ----------

# DBTITLE 1,Sample Customer Metrics
# MAGIC %sql
# MAGIC -- Top 10 customers by order volume
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   total_orders,
# MAGIC   avg_review_score,
# MAGIC   delivered_orders,
# MAGIC   canceled_orders,
# MAGIC   customer_lifetime_days
# MAGIC FROM main.gold.customer_order_summary
# MAGIC ORDER BY total_orders DESC
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Sample Order Performance
# MAGIC %sql
# MAGIC -- Order performance trends by month
# MAGIC SELECT 
# MAGIC   order_year,
# MAGIC   order_month,
# MAGIC   order_status,
# MAGIC   order_count,
# MAGIC   avg_approval_delay_hours,
# MAGIC   avg_delivery_delay_days
# MAGIC FROM main.gold.order_performance_metrics
# MAGIC WHERE order_year = 2025
# MAGIC ORDER BY order_year DESC, order_month DESC, order_count DESC
# MAGIC LIMIT 15

# COMMAND ----------

# DBTITLE 1,Sample Review Sentiment
# MAGIC %sql
# MAGIC -- Review sentiment distribution
# MAGIC SELECT 
# MAGIC   review_year,
# MAGIC   review_month,
# MAGIC   review_sentiment,
# MAGIC   review_count,
# MAGIC   sentiment_percentage,
# MAGIC   avg_review_score
# MAGIC FROM main.gold.review_analytics
# MAGIC ORDER BY review_year DESC, review_month DESC, review_sentiment
# MAGIC LIMIT 15

# COMMAND ----------

# DBTITLE 1,Sample Geographic Performance
# MAGIC %sql
# MAGIC -- Top performing states by order volume and delivery rate
# MAGIC SELECT 
# MAGIC   customer_state,
# MAGIC   SUM(total_orders) as total_orders,
# MAGIC   SUM(unique_customers) as total_customers,
# MAGIC   AVG(avg_review_score) as avg_review_score,
# MAGIC   AVG(delivery_rate) as avg_delivery_rate,
# MAGIC   AVG(cancellation_rate) as avg_cancellation_rate
# MAGIC FROM main.gold.geographic_analysis
# MAGIC GROUP BY customer_state
# MAGIC ORDER BY total_orders DESC
# MAGIC LIMIT 10

# COMMAND ----------

