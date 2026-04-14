from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, when, to_timestamp, regexp_replace

# SPARK SESSION + DELTA
spark = SparkSession.builder.appName("Silver-Layer-Final-Clean").getOrCreate()

# DATASET 1: DATACO
df = spark.read.format("delta").load("gs://bigdata-spark-bucket/bronze/dataco/")

# 1. DROP COLUMNS KHÔNG CẦN THIẾT
drop_cols = ["order_zipcode", "customer_email"]
for c in drop_cols:
    if c in df.columns:
        df = df.drop(c)

# 2. HANDLE MISSING
df = df.fillna({"customer_lname": "unknown"})

# 3. CLEAN ORDER CITY / COUNTRY
# Loại bỏ ký tự lạ, chỉ giữ chữ và space
df = df.withColumn("order_country", regexp_replace(col("order_country"), r"[^a-zA-Z\s]", "")) \
       .withColumn("order_city", regexp_replace(col("order_city"), r"[^a-zA-Z\s]", ""))

df = df.withColumn("order_country", trim(col("order_country"))) \
       .withColumn("order_city", trim(col("order_city")))

# 4. REMOVE NOISE: order_country là số set null
df = df.withColumn("order_country",
                   when(col("order_country").rlike("^[0-9]+$"), None)
                   .otherwise(col("order_country")))

# 5. STANDARDIZE COUNTRY
df = df.withColumn("order_country",
    when(col("order_country").like("%Turqu%"), "Turkey")
    .when(col("order_country").like("%replica%congo%"), "Democratic Republic of the Congo")
    .when(col("order_country").isin("Estados Unidos", "USA"), "United States")
    .when(col("order_country").isin("Francia"), "France")
    .when(col("order_country").like("%Mxico%"), "Mexico")
    .when(col("order_country").like("%Jap%"), "Japan")
    .when(col("order_country").like("%Pakist%"), "Pakistan")
    .otherwise(col("order_country"))
)

# 6. CLEAN TEXT COLUMNS
text_cols = [
    "customer_city", "customer_state",
    "order_city", "order_state",
    "category_name", "department_name",
    "market", "order_status"
]
for c in text_cols:
    df = df.withColumn(c, lower(trim(col(c))))

# 7. CLEAN DATE COLUMNS
df = df.filter(col("order_date_dateorders").isNotNull()) \
       .filter(col("shipping_date_dateorders").isNotNull())

df = df.withColumn("order_date", to_timestamp(col("order_date_dateorders"), "M/d/yyyy H:mm")) \
       .withColumn("shipping_date", to_timestamp(col("shipping_date_dateorders"), "M/d/yyyy H:mm"))

# 8. DROP DUPLICATES
df = df.dropDuplicates(["order_id", "order_item_id"])

# 9. BUSINESS LOGIC SILVER LAYER
df = df.filter(col("sales") > 0) \
       .filter(col("order_item_quantity") > 0) \
       .filter(col("order_status").isin("complete", "closed"))

# 10. WRITE SILVER DATACO
df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("gs://bigdata-spark-bucket/silver/dataco/")

# DATASET 2: LOGS
df_log = spark.read.format("delta").load("gs://bigdata-spark-bucket/bronze/logs/")

# DROP DUPLICATES
df_log = df_log.dropDuplicates()

# CONVERT EVENT TIME
df_log = df_log.filter(col("event_time").isNotNull())
df_log = df_log.withColumn("event_time", to_timestamp(col("event_time"), "M/d/yyyy H:mm"))

# FILTER HOUR VALID
df_log = df_log.filter(col("hour").between(0, 23))

# SIMPLE FEATURE: USER REGION / IS NIGHT
df_log = df_log.withColumn("user_region", col("ip").substr(1, 3)) \
               .withColumn("is_night", when(col("hour") < 6, 1).otherwise(0))

# WRITE SILVER LOGS
df_log.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("gs://bigdata-spark-bucket/silver/logs/")

print("SILVER CLEANING COMPLETE")

spark.stop()