from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("Silver to Gold").getOrCreate()

#Đọc dữ liệu Silver từ GCS
silver_path = "gs://bigdata-spark-bucket/silver/dataco/"
df = spark.read.format("delta").load(silver_path)

# ============================================================
# 1. KPI TỔNG HỢP THEO THỊ TRƯỜNG & DANH MỤC
#    - Doanh thu, lợi nhuận, số đơn, tỷ lệ giao trễ
#    - Tỷ lệ chiết khấu TB, profit ratio TB, số khách hàng unique, giá trị đơn hàng TB
# ============================================================
gold_kpi_df = df.groupBy("market", "category_name").agg(
    F.sum("sales").alias("total_sales"),
    F.sum("order_profit_per_order").alias("total_profit"),
    F.count("order_id").alias("total_orders"),
    F.sum("late_delivery_risk").alias("total_late_risks"),
    F.avg("order_item_discount_rate").alias("avg_discount_rate"),
    F.avg("order_item_profit_ratio").alias("avg_profit_ratio"),
    F.countDistinct("customer_id").alias("unique_customers"),
    F.avg("sales").alias("avg_order_value"),
).withColumn(
    "late_delivery_rate",
    F.round((F.col("total_late_risks") / F.col("total_orders")) * 100, 2)
).withColumn(
    "avg_discount_rate",
    F.round(F.col("avg_discount_rate") * 100, 2)
).withColumn(
    "avg_profit_ratio",
    F.round(F.col("avg_profit_ratio") * 100, 2)
)

# ============================================================
# 2. PHÂN TÍCH DOANH THU THEO THÁNG & THỊ TRƯỜNG
# ============================================================

monthly_analytics_df = (
    df.withColumn(
        "order_timestamp",
        F.to_timestamp("order_date_dateorders", "M/d/yyyy H:mm")
    )
    .withColumn("order_month", F.date_format("order_timestamp", "yyyy-MM"))
    .groupBy("order_month", "market")
    .agg(
        F.sum("sales").alias("monthly_revenue"),
        F.sum("order_profit_per_order").alias("monthly_profit"),
        F.count("order_id").alias("total_orders"),
        F.avg("sales").alias("avg_order_value"),
    )
    .withColumn(
        "profit_margin_pct",
        F.round((F.col("monthly_profit") / F.col("monthly_revenue")) * 100, 2)
    )
    .orderBy("order_month")
)
 
# ============================================================
# 3. HIỆU SUẤT VẬN CHUYỂN THEO PHƯƠNG THỨC & VÙNG
#    - Số đơn giao trễ thực tế, tỷ lệ giao trễ, tổng số đơn hàng
# ============================================================
shipping_performance_df = df.groupBy("shipping_mode", "order_region").agg(
    F.avg("days_for_shipping_real").alias("avg_actual_shipping_days"),
    F.avg("days_for_shipment_scheduled").alias("avg_scheduled_shipping_days"),
    F.count("order_id").alias("total_shipments"),
    F.sum(
        F.when(
            F.col("days_for_shipping_real") > F.col("days_for_shipment_scheduled"), 1
        ).otherwise(0)
    ).alias("late_shipment_count"),
).withColumn(
    "on_time_failure_rate",
    F.round((F.col("late_shipment_count") / F.col("total_shipments")) * 100, 2)
).withColumn(
    "avg_delay_days",
    F.round(
        F.col("avg_actual_shipping_days") - F.col("avg_scheduled_shipping_days"), 2
    )
)
 
# ============================================================
# 4. PHÂN TÍCH KHÁCH HÀNG THEO PHÂN KHÚC & THỊ TRƯỜNG
# ============================================================
customer_analytics_df = df.groupBy("customer_segment", "market").agg(
    F.countDistinct("customer_id").alias("total_customers"),
    F.avg("sales_per_customer").alias("avg_revenue_per_customer"),
    F.sum("sales").alias("segment_revenue"),
    F.sum("order_profit_per_order").alias("segment_profit"),
    F.count("order_id").alias("total_orders"),
    F.sum("late_delivery_risk").alias("total_late_risks"),
).withColumn(
    "late_delivery_rate",
    F.round((F.col("total_late_risks") / F.col("total_orders")) * 100, 2)
).withColumn(
    "avg_revenue_per_customer",
    F.round(F.col("avg_revenue_per_customer"), 2)
)
 
# ============================================================
# 5. GHI DỮ LIỆU VÀO GOLD LAYER 
# ============================================================
gold_kpi_path          = "gs://bigdata-spark-bucket/gold/kpi_summary"
gold_monthly_path      = "gs://bigdata-spark-bucket/gold/monthly_financials"
gold_shipping_path     = "gs://bigdata-spark-bucket/gold/shipping_performance"
gold_customer_path     = "gs://bigdata-spark-bucket/gold/customer_analytics"
 
print("[INFO] Dang ghi du lieu xuong tang Gold...")
 
gold_kpi_df.write.format("delta").mode("overwrite").save(gold_kpi_path)
print(f"  [OK] kpi_summary            -> {gold_kpi_df.count()} dong")
 
monthly_analytics_df.write.format("delta").mode("overwrite").save(gold_monthly_path)
print(f"  [OK] monthly_financials     -> {monthly_analytics_df.count()} dong")
 
shipping_performance_df.write.format("delta").mode("overwrite").save(gold_shipping_path)
print(f"  [OK] shipping_performance   -> {shipping_performance_df.count()} dong")
 
customer_analytics_df.write.format("delta").mode("overwrite").save(gold_customer_path)
print(f"  [OK] customer_analytics     -> {customer_analytics_df.count()} dong")
 
# ============================================================
# 6. TỐI ƯU HÓA: OPTIMIZE + Z-ORDER + VACUUM
# ============================================================
VACUUM_RETAIN_HOURS = 168  # Giữ lại 7 ngày lịch sử Delta
 
print("[INFO] Dang thuc hien Optimize, Z-Order va Vacuum cho cac bang Gold...")
 
# --- KPI Summary ---
kpi_table = DeltaTable.forPath(spark, gold_kpi_path)
kpi_table.optimize().executeZOrderBy("market")
kpi_table.vacuum(VACUUM_RETAIN_HOURS)
print("  [OK] kpi_summary: Optimize + Z-Order(market) + Vacuum")
 
# --- Monthly Financials ---
monthly_table = DeltaTable.forPath(spark, gold_monthly_path)
monthly_table.optimize().executeZOrderBy("order_month")
monthly_table.vacuum(VACUUM_RETAIN_HOURS)
print("  [OK] monthly_financials: Optimize + Z-Order(order_month) + Vacuum")
 
# --- Shipping Performance ---
shipping_table = DeltaTable.forPath(spark, gold_shipping_path)
shipping_table.optimize().executeZOrderBy("shipping_mode")
shipping_table.vacuum(VACUUM_RETAIN_HOURS)
print("  [OK] shipping_performance: Optimize + Z-Order(shipping_mode) + Vacuum")
 
# --- Customer Analytics ---
customer_table = DeltaTable.forPath(spark, gold_customer_path)
customer_table.optimize().executeZOrderBy("customer_segment")
customer_table.vacuum(VACUUM_RETAIN_HOURS)
print("  [OK] customer_analytics: Optimize + Z-Order(customer_segment) + Vacuum")
 
print("[INFO] Hoan thanh ETL Silver -> Gold va Toi uu hoa du lieu.")
 
# Ngắt kết nối
spark.stop()