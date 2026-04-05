from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, when, to_timestamp,
    regexp_replace
)

# 1. SPARK + DELTA
spark = SparkSession.builder \
    .appName("Silver-Layer-Processing-Final") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 2. CONFIG GCS
spark._jsc.hadoopConfiguration().set(
    "google.cloud.auth.service.account.enable", "true"
)
spark._jsc.hadoopConfiguration().set(
    "google.cloud.auth.service.account.json.keyfile",
    "/opt/keys/key.json"
)

print("===== START SILVER PROCESSING =====")

# =====================================================
# 🟡 DATASET 1: DATACO
# =====================================================

print("Loading Bronze DataCo...")

df = spark.read.format("delta").load(
    "gs://bigdata-spark-bucket/bronze/dataco/"
)

# 1. DROP BAD COLUMNS
df = df.drop("product_description", "order_zipcode")

# 2. HANDLE MISSING
df = df.fillna({
    "customer_lname": "unknown",
    "customer_zipcode": 0
})

# 3. FIX ENCODING (QUAN TRỌNG)
df = df \
    .withColumn("order_country",
        regexp_replace(col("order_country"), r"[^a-zA-Z\s]", "")) \
    .withColumn("order_city",
        regexp_replace(col("order_city"), r"[^a-zA-Z\s]", ""))

# 4. TRIM SAU KHI CLEAN
df = df \
    .withColumn("order_country", trim(col("order_country"))) \
    .withColumn("order_city", trim(col("order_city")))

# 5. STANDARDIZE COUNTRY
df = df.withColumn("order_country", 
    when(col("order_country").isin("Estados Unidos", "USA"), "United States")
    .when(col("order_country").isin("Francia"), "France")
    .when(col("order_country").like("%viet%"), "Vietnam")
    .when(col("order_country").like("%afgan%"), "Afghanistan")
    .when(col("order_country").like("%jap%"), "Japan")
    .when(col("order_country").like("%corea%"), "South Korea")
    .otherwise(col("order_country"))
)

# 6. CLEAN TEXT
text_cols = [
    "customer_city", "customer_state",
    "order_city", "order_state",
    "category_name", "department_name",
    "market", "order_status"
]

for c in text_cols:
    df = df.withColumn(c, lower(trim(col(c))))

# 7. CONVERT DATE
df = df \
    .withColumn("order_date",
                to_timestamp(col("order_date_dateorders"),
                             "M/d/yyyy H:mm")) \
    .withColumn("shipping_date",
                to_timestamp(col("shipping_date_dateorders"),
                             "M/d/yyyy H:mm"))

# 8. REMOVE DUPLICATES
df = df.dropDuplicates(["order_id", "order_item_id"])

# 9. BUSINESS LOGIC
df = df \
    .filter(col("sales") > 0) \
    .filter(col("order_item_quantity") > 0) \
    .filter(col("order_status").isin("complete", "closed"))

# 10. FEATURE ENGINEERING
df = df \
    .withColumn("total_revenue",
        col("order_item_quantity") * col("order_item_product_price")) \
    .withColumn("profit_flag",
        when(col("order_profit_per_order") > 0, 1).otherwise(0)) \
    .withColumn("shipping_delay",
        col("days_for_shipping_real") - col("days_for_shipment_scheduled"))

# 11. DATA QUALITY
df = df.filter(col("total_revenue") > 0)

print("Writing Silver DataCo...")

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("gs://bigdata-spark-bucket/silver/dataco/")

# =====================================================
# 🔵 DATASET 2: LOGS
# =====================================================

print("Processing Logs...")

df_log = spark.read.format("delta").load(
    "gs://bigdata-spark-bucket/bronze/logs/"
)

# Clean duplicates
df_log = df_log.dropDuplicates()

# Convert time
df_log = df_log.withColumn(
    "event_time",
    to_timestamp(col("event_time"), "M/d/yyyy H:mm")
)

# Business logic
df_log = df_log.filter(col("hour").between(0, 23))

# Feature engineering
df_log = df_log \
    .withColumn("user_region", col("ip").substr(1, 3)) \
    .withColumn("is_night",
        when(col("hour") < 6, 1).otherwise(0))

print("Writing Silver Logs...")

df_log.write \
    .format("delta") \
    .mode("overwrite") \
    .save("gs://bigdata-spark-bucket/silver/logs/")

print("===== ✅ SILVER PROCESSING DONE =====")

spark.stop()