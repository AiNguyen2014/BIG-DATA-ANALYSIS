1. Clone repo
2. Copy file .env.example thành file .env, đổi tên của mình
3. Bỏ file key GCP vào config/gcs/credentials/
4. Tạo thư mục packages chứa các file nén hadoop, spark, ...
5. Chạy:
   docker compose up -d --build
6. Khi chạy spark với delta-lake sử dụng lệnh: spark-submit \
--master yarn \
--deploy-mode cluster \
--packages io.delta:delta-spark_2.13:4.0.0 \
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
jobs/bronze_to_silver_job.py #thay bằng file pyspark của mình
