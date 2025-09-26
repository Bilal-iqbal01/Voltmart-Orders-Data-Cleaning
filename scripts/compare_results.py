from pyspark.sql import SparkSession
import os

# -----------------------------------------------------------------------------
# Initialize Spark
# -----------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("compare_cleaned_results")
    .getOrCreate()
)

# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------
api_path = os.path.join("outputs", "orders_data_clean_api.parquet")
sql_path = os.path.join("outputs", "orders_data_clean_sql.parquet")

# -----------------------------------------------------------------------------
# Load datasets
# -----------------------------------------------------------------------------
df_api_cleaned = spark.read.parquet(api_path)
df_sql_cleaned = spark.read.parquet(sql_path)

# -----------------------------------------------------------------------------
# Common columns
# -----------------------------------------------------------------------------
common_columns = [
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
]

df_api_cleaned = df_api_cleaned.select(*common_columns)
df_sql_cleaned = df_sql_cleaned.select(*common_columns)

# -----------------------------------------------------------------------------
# Compare row counts
# -----------------------------------------------------------------------------
print("👉 PySpark API row count:", df_api_cleaned.count())
print("👉 SQL row count:", df_sql_cleaned.count())

# -----------------------------------------------------------------------------
# Compare differences (exact match, including duplicates)
# -----------------------------------------------------------------------------
diff = df_api_cleaned.exceptAll(df_sql_cleaned).union(df_sql_cleaned.exceptAll(df_api_cleaned))

print("👉 Number of differing rows:", diff.count())

if diff.count() == 0:
    print("✅ Both cleaned datasets match perfectly!")
else:
    print("❌ Differences found between the datasets")
    diff.show(10, truncate=False)
