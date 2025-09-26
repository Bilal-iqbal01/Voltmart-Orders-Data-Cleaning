from pyspark.sql import SparkSession, functions as F
import os

# -----------------------------------------------------------------------------
# Initialize Spark
# -----------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("cleaning_orders_dataset_with_pyspark")
    .getOrCreate()
)

# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------
input_path = os.path.join("data", "orders_data.parquet")
output_path = os.path.join("outputs", "orders_data_clean_api.parquet")

# -----------------------------------------------------------------------------
# Load dataset
# -----------------------------------------------------------------------------
orders_df = spark.read.parquet(input_path)

# -----------------------------------------------------------------------------
# Clean and transform dataset
# -----------------------------------------------------------------------------
orders_cleaned_pyspark = (
    orders_df
    # Keep orders placed from 5 AM onwards
    .filter(F.hour("order_date") >= 5)

    # Create 'time_of_day' column
    .withColumn(
        "time_of_day",
        F.when((F.hour("order_date") >= 5) & (F.hour("order_date") < 12), "morning")
         .when((F.hour("order_date") >= 12) & (F.hour("order_date") < 18), "afternoon")
         .when((F.hour("order_date") >= 18) & (F.hour("order_date") < 24), "evening")
    )

    # Convert 'order_date' to date
    .withColumn("order_date", F.col("order_date").cast("date"))

    # Clean 'product': lowercase + trim, remove TVs
    .withColumn("product", F.lower(F.trim(F.col("product"))))
    .filter(~F.col("product").like("%tv%"))

    # Lowercase 'category'
    .withColumn("category", F.lower(F.col("category")))

    # Extract 'purchase_state' (2nd to last token from address)
    .withColumn(
        "purchase_state",
        F.element_at(F.split(F.col("purchase_address"), " "), -2)
    )

    # Reorder columns
    .select(
        "order_date",
        "time_of_day",
        "order_id",
        "product",
        "product_id",
        "category",
        "purchase_address",
        "purchase_state",
        "quantity_ordered",
        "price_each",
        "cost_price",
        "turnover",
        "margin"
    )
)

# -----------------------------------------------------------------------------
# Save output
# -----------------------------------------------------------------------------
orders_cleaned_pyspark.write.mode("overwrite").parquet(output_path)

print(f"✅ Cleaned dataset (PySpark API) saved to {output_path}")
