# 1. Initialize Unity Catalog Schemas and Volume
spark.sql("CREATE CATALOG IF NOT EXISTS main")
spark.sql("CREATE SCHEMA IF NOT EXISTS main.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS main.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS main.gold")
spark.sql("CREATE VOLUME IF NOT EXISTS main.default.raw_landing")

import pandas as pd
import json
from datetime import datetime, timedelta
import random

BASE_VOL = "/Volumes/main/default/raw_landing"
dbutils.fs.mkdirs(f"{BASE_VOL}/orders")
dbutils.fs.mkdirs(f"{BASE_VOL}/reviews")
dbutils.fs.mkdirs(f"{BASE_VOL}/customers")

# 2. Generate Mock E-Commerce Datasets
print("Generating mock datasets into Volume...")

# Source 1: Orders (CSV Batch / CDC Feed)
orders_data = []
for i in range(1000):
    order_date = datetime(2025, 1, 1) + timedelta(days=random.randint(0, 365))
    orders_data.append({
        "order_id": f"ORD{str(i+1).zfill(6)}",
        "customer_id": f"CUST{random.randint(1, 300)}",
        "order_status": random.choice(["delivered", "shipped", "processing", "canceled"]),
        "order_purchase_timestamp": order_date.isoformat(),
        "order_approved_at": (order_date + timedelta(hours=2)).isoformat(),
        "order_delivered_carrier_date": (order_date + timedelta(days=3)).isoformat(),
        "order_delivered_customer_date": (order_date + timedelta(days=7)).isoformat(),
        "order_estimated_delivery_date": (order_date + timedelta(days=10)).isoformat()
    })
pd.DataFrame(orders_data).to_csv(f"{BASE_VOL}/orders/olist_orders.csv", index=False)

# Source 2: Customers Master (CSV Dimension)
customers_data = []
for i in range(300):
    customers_data.append({
        "customer_id": f"CUST{i+1}",
        "customer_unique_id": f"UNIQ{i+1}",
        "customer_zip_code_prefix": random.randint(10000, 99999),
        "customer_city": random.choice(["São Paulo", "Rio de Janeiro", "Brasília", "Salvador", "Fortaleza"]),
        "customer_state": random.choice(["SP", "RJ", "DF", "BA", "CE"])
    })
pd.DataFrame(customers_data).to_csv(f"{BASE_VOL}/customers/olist_customers.csv", index=False)

# Source 3: Reviews (Streaming JSON Payload Simulation)
reviews_data = []
for i in range(500):
    review_date = datetime(2025, 1, 1) + timedelta(days=random.randint(0, 365))
    reviews_data.append({
        "review_id": f"REV{str(i+1).zfill(6)}",
        "order_id": f"ORD{random.randint(1, 1000)}",
        "review_score": random.randint(1, 5),
        "review_comment_title": random.choice(["Great!", "Good", "OK", "Bad", "Terrible", None]),
        "review_comment_message": random.choice(["Loved it", "Fast delivery", "As expected", "Disappointing", None]),
        "review_creation_date": review_date.isoformat(),
        "review_answer_timestamp": (review_date + timedelta(days=1)).isoformat()
    })
dbutils.fs.put(f"{BASE_VOL}/reviews/streaming_reviews_batch1.json", 
               "\n".join([json.dumps(review) for review in reviews_data]), overwrite=True)

print("✅ Setup complete. Unity Catalog schemas and volume landing files ready.")

