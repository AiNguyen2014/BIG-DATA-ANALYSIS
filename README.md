Cấu trúc thư mục:
```text
HADOOP_CLUSTER/
├── config/
│   ├── gcs/
│   │   └── credentials/
│   │       └── bigdata-spark-project-....json
│   ├── hadoop/
│   │   ├── capacity-scheduler.xml
│   │   ├── core-site.xml
│   │   ├── hadoop-env.sh
│   │   ├── hdfs-site.xml
│   │   ├── log4j.properties
│   │   ├── mapred-site.xml
│   │   ├── workers
│   │   └── yarn-site.xml
│   └── spark/
│       ├── spark-defaults.conf
│       └── spark-env.sh
├── data/
│   ├── datanode/
│   └── namenode/
├── docker/
│   ├── Dockerfile.hadoop
│   └── Dockerfile.spark
├── jobs/
│   └── bronze_to_silver_job.py
├── packages/
│   ├── gcs-connector-hadoop3-latest.jar
│   ├── hadoop-3.4.3.tar.gz
│   ├── jdk-21_linux-x64_bin.tar.gz
│   └── spark-4.0.2-bin-hadoop3.tgz
├── scripts/
│   ├── entrypoint.sh
│   ├── start-master.sh
│   └── start-worker.sh
├── .env
├── .env.example
├── .gitignore
├── docker-compose.yml
└── README.md

```         
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

File bronze_to_silver_job.py đã thực hiện etl bước đầu, xem để thực hiện tiếp.

Thành viên B có thể load các file này xử lý tiếp:
```text
Dữ liệu	            Đường dẫn trên GCS	                     Mục đích
Supply Chain Bronze	gs://bigdata-spark-bucket/bronze/dataco/	load về xử lý Silver
Logs Bronze	         gs://bigdata-spark-bucket/bronze/logs/	   load về xử lý Silver

