from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    split,
    to_timestamp
)

# 1. CREATE SPARK SESSION

spark = SparkSession.builder \
    .appName("Lakehouse-Bronze-To-Silver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# DATASET 1: DATACO SUPPLY CHAIN

print("Loading DataCo dataset from GCS...")

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("gs://bigdata-spark-bucket/DataCoSupplyChainDataset.csv")

# Rename columns

new_columns = [
    c.lower()
    .replace(" ", "_")
    .replace("(", "")
    .replace(")", "")
    for c in df.columns
]

df = df.toDF(*new_columns)

# Cast data types

df = df \
    .withColumn("sales", col("sales").cast("double")) \
    .withColumn("order_item_quantity", col("order_item_quantity").cast("int")) \
    .withColumn("order_item_product_price", col("order_item_product_price").cast("double")) \
    .withColumn("order_profit_per_order", col("order_profit_per_order").cast("double")) \
    .withColumn("latitude", col("latitude").cast("double")) \
    .withColumn("longitude", col("longitude").cast("double"))

# Remove null values

df = df.dropna(subset=[
    "customer_id",
    "order_id",
    "sales"
])

# Remove duplicates

df = df.dropDuplicates()

# Drop unnecessary columns

df = df.drop(
    "customer_password",
    "product_description",
    "product_image"
)

# Save Bronze layer (Delta)

print("Writing Bronze layer...")

df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("gs://bigdata-spark-bucket/bronze/dataco/")

# Register Bronze Table

spark.sql("""
CREATE TABLE IF NOT EXISTS dataco_bronze
USING DELTA
LOCATION 'gs://bigdata-spark-bucket/bronze/dataco/'
""")

# Create Silver layer

print("Creating Silver layer...")

df_bronze = spark.read \
    .format("delta") \
    .load("gs://bigdata-spark-bucket/bronze/dataco/")

df_silver = df_bronze \
    .filter(col("sales") > 0) \
    .filter(col("order_item_quantity") > 0) \
    .filter(col("order_status").isNotNull())

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save("gs://bigdata-spark-bucket/silver/dataco/")

spark.sql("""
CREATE TABLE IF NOT EXISTS dataco_silver
USING DELTA
LOCATION 'gs://bigdata-spark-bucket/silver/dataco/'
""")

# DATASET 2: ACCESS LOG DATASET

print("Loading Access Logs dataset...")

df_log = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("gs://bigdata-spark-bucket/tokenized_access_logs.csv")

# Rename columns

df_log = df_log.toDF(
    "product",
    "category",
    "event_time",
    "month",
    "hour",
    "department",
    "ip",
    "url"
)

# Clean text fields

df_log = df_log \
    .withColumn("product", trim(lower(col("product")))) \
    .withColumn("category", trim(lower(col("category")))) \
    .withColumn("department", trim(lower(col("department"))))

# Cast timestamp and numeric

df_log = df_log \
    .withColumn("event_time", to_timestamp("event_time", "M/d/yyyy H:mm")) \
    .withColumn("hour", col("hour").cast("int"))

# Remove null

df_log = df_log.dropna(subset=[
    "product",
    "event_time",
    "ip"
])

# Remove duplicates

df_log = df_log.dropDuplicates()

# Feature engineering


df_log = df_log \
    .withColumn("url_path", split(col("url"), "/")) \
    .withColumn("user_region", split(col("ip"), "\\.")[0])

# Save Bronze layer

df_log.write \
    .format("delta") \
    .mode("overwrite") \
    .save("gs://bigdata-spark-bucket/bronze/logs/")

# Create Silver logs

df_log_silver = df_log \
    .filter(col("event_time").isNotNull()) \
    .filter(col("product").isNotNull())

df_log_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save("gs://bigdata-spark-bucket/silver/logs/")

print("Pipeline Bronze -> Silver completed.")

spark.stop()