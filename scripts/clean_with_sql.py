from pyspark.sql import SparkSession
import os

# -----------------------------------------------------------------------------
# Initialize Spark
# -----------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("cleaning_orders_dataset_with_sql")
    .getOrCreate()
)

# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------
input_path = os.path.join("data", "orders_data.parquet")
output_path = os.path.join("outputs", "orders_data_clean_sql.parquet")

# -----------------------------------------------------------------------------
# Load dataset and register as SQL view
# -----------------------------------------------------------------------------
orders_df = spark.read.parquet(input_path)
orders_df.createOrReplaceTempView("orders")

# -----------------------------------------------------------------------------
# SQL query for cleaning
# -----------------------------------------------------------------------------
query = """
SELECT
    CAST(order_date AS DATE) AS order_date,
    CASE
        WHEN HOUR(order_date) >= 5 AND HOUR(order_date) < 12 THEN 'morning'
        WHEN HOUR(order_date) >= 12 AND HOUR(order_date) < 18 THEN 'afternoon'
        WHEN HOUR(order_date) >= 18 AND HOUR(order_date) < 24 THEN 'evening'
    END AS time_of_day,
    order_id,
    LOWER(TRIM(product)) AS product,
    product_id,
    LOWER(category) AS category,
    purchase_address,
    SPLIT(purchase_address, ' ')[SIZE(SPLIT(purchase_address, ' ')) - 2] AS purchase_state,
    quantity_ordered,
    price_each,
    cost_price,
    turnover,
    margin
FROM orders
WHERE HOUR(order_date) >= 5
  AND LOWER(product) NOT LIKE '%tv%'
"""

orders_cleaned_sql = spark.sql(query)

# -----------------------------------------------------------------------------
# Save output
# -----------------------------------------------------------------------------
orders_cleaned_sql.write.mode("overwrite").parquet(output_path)

print(f"✅ Cleaned dataset (SQL) saved to {output_path}")
