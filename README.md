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
│   |── bronze_to_silver_job.py
|___|__silver_to_gold_job.py
|
├── packages/
│   ├── gcs-connector-hadoop3-latest.jar
│   ├── hadoop-3.4.3.tar.gz
│   ├── jdk-21_linux-x64_bin.tar.gz
│   └── spark-4.0.2-bin-hadoop3.tgz
├── scripts/
│   ├── entrypoint.sh
│   ├── start-master.sh
│   └── start-worker.sh
├── logs 
├── .env
├── .env.example
├── .gitignore
├── docker-compose.yml
└── README.md

```         
1. Clone repo
2. Copy file .env.example thành file .env, đổi USERNAME trong file .env thành tên của mình. VD:hadoopainguyen
3. Bỏ file key GCP vào config/gcs/credentials/
4. Tạo thư mục packages chứa các file nén hadoop, spark, ...Tạo thêm thư mục rỗng là logs
5. Chạy:
   docker-compose build
- Khởi tạo database Airflow (BẮT BUỘC)
docker-compose run airflow-webserver airflow db init
- Tạo user đăng nhập Airflow
docker-compose run airflow-webserver airflow users create --username admin --password admin --firstname admin --lastname admin --role Admin --email admin@email.com
- CHẠY toàn bộ hệ thống
docker-compose up -d

6. Khi chạy spark với delta-lake sử dụng lệnh: 
Lưu ý: cần vào container để chạy spark bằng lệnh 

docker exec -it hadoop-master bash   (lệnh này sẽ vào root)

Nên đăng nhập bằng user trước khi chạy spark-submit

su - hadoopainguyen

Sau đó mới chạy spark-submit khi cần
spark-submit \
--master yarn \
--deploy-mode cluster \
jobs/bronze_to_silver_job.py #thay bằng file pyspark của mình


Thành viên B có thể load các file này xử lý tiếp:
```text
Dữ liệu	            Đường dẫn trên GCS	                     Mục đích
Supply Chain Bronze	gs://bigdata-spark-bucket/bronze/dataco/	load về xử lý Silver
Logs Bronze	         gs://bigdata-spark-bucket/bronze/logs/	   load về xử lý Silver

Dữ liệu
Từ tầng Gold:        gold_kpi_path = "gs://bigdata-spark-bucket/gold/kpi_summary"
                     gold_monthly_path = "gs://bigdata-spark-bucket/gold/monthly_financials"
                     gold_shipping_path = "gs://bigdata-spark-bucket/gold/shipping_performance"
                     gold_customer_path = "gs://bigdata-spark-bucket/gold/customer_analytics"

# Hướng dẫn — Phần D (Metadata + Visualization)
## Khởi động

```bash
docker-compose up -d hive-metastore spark-thrift
```

Kiểm tra đang chạy:

```bash
docker ps | grep -E "hive-metastore|spark-thrift"
```

→ Phải thấy cả 2 container ở trạng thái **Up**

8. Triển khai Apache Superset

> **Bước này BẮT BUỘC chạy thủ công 1 lần trên mỗi máy**, không tự động khi `docker compose up`.

Khởi động container Superset:

```bash
docker-compose up -d superset
```

Khởi tạo database Superset:

```bash
docker exec -it superset-new superset db upgrade
docker exec -it superset-new superset init
```

Tạo user đăng nhập:

```bash
docker exec -it superset-new superset fab create-admin --username admin --firstname Admin --lastname User --email admin@example.com --password admin123
```

Truy cập Superset tại **http://localhost:8089** · Đăng nhập: `admin / admin123`

9. Đăng ký dữ liệu Gold vào Hive Metastore

```bash
docker exec -it hadoop-master bash
```

Trong container, chạy spark-sql:

```bash
spark-sql --master local --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083
```

Trong spark-sql shell, chạy lần lượt:

```sql
CREATE DATABASE IF NOT EXISTS gold;

CREATE EXTERNAL TABLE IF NOT EXISTS gold.kpi_summary
  USING DELTA LOCATION 'gs://bigdata-spark-bucket/gold/kpi_summary';

CREATE EXTERNAL TABLE IF NOT EXISTS gold.monthly_financials
  USING DELTA LOCATION 'gs://bigdata-spark-bucket/gold/monthly_financials';

CREATE EXTERNAL TABLE IF NOT EXISTS gold.shipping_performance
  USING DELTA LOCATION 'gs://bigdata-spark-bucket/gold/shipping_performance';

CREATE EXTERNAL TABLE IF NOT EXISTS gold.customer_analytics
  USING DELTA LOCATION 'gs://bigdata-spark-bucket/gold/customer_analytics';
```

Xác minh: Superset → SQL → SQL Lab → chọn database `gold` → chạy:

```sql
SHOW TABLES IN gold
```

→ Phải thấy đủ 4 bảng: `kpi_summary`, `monthly_financials`, `shipping_performance`, `customer_analytics`

10. Kết nối Superset với Spark Thrift Server

Cài driver pyhive vào Superset:
```bash
docker exec -u root superset-new /app/.venv/bin/python -m ensurepip
docker exec -u root superset-new /app/.venv/bin/python -m pip install pyhive thrift thrift-sasl
```
Cấu hình GCS connector cho Spark Thrift:
```bash
docker exec -u root spark-thrift bash -c "mkdir -p /opt/spark/conf && cat > /opt/spark/conf/core-site.xml << EOF
<?xml version=\"1.0\"?>
<configuration>
  <property><name>fs.gs.impl</name><value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem</value></property>
  <property><name>google.cloud.auth.service.account.enable</name><value>true</value></property>
  <property><name>google.cloud.auth.service.account.json.keyfile</name><value>/opt/keys/key.json</value></property>
</configuration>
EOF"
```
```bash
docker cp /tmp/gcs.jar spark-thrift:/opt/spark/jars/gcs-connector.jar
docker restart spark-thrift
```
Thêm kết nối database trong Superset:
- Vào **Settings → Database Connections → + Database → Other**
- Nhập SQLAlchemy URI:

```
hive://spark-thrift:10000/gold
```
- Bấm **Test Connection** → phải thấy "Connection looks good!" → **Save**

11. Kiểm tra Dashboard

Vào Superset → **Dashboards** → mở **"Supply Chain Analytics Dashboard"**

→ Phải thấy 4 charts hiển thị đầy đủ dữ liệu:

| Chart | Dataset | Loại |
|-------|---------|------|
| Total Sales by Market & Category | kpi_summary | Bar Chart |
| Monthly Revenue by Market | monthly_financials | Line Chart |
| Shipments by Region & Mode | shipping_performance | Bar Chart |
| Customer Segment Distribution | customer_analytics | Pie Chart |

12. Kiểm tra DAG Airflow tự động refresh

Vào Airflow UI tại **http://localhost:8081** → tìm DAG tên `superset_dashboard_refresh` (tag: `phan-D`)

- Tab **Details**: `Has import errors = false`, `Total Tasks = 4`
- Tab **Graph**: `wait_for_bronze_silver_gold` → `check_superset_health` → `refresh_gold_datasets` → `warmup_dashboard_cache`

## Các service

| Service | URL | Tài khoản |
|---------|-----|-----------|
| Superset | http://localhost:8089 | admin / admin123 |
| Airflow | http://localhost:8081 | admin / admin |
| Hadoop HDFS | http://localhost:9870 | — |
| YARN | http://localhost:8088 | — |

Checklist kiểm tra toàn bộ hệ thống

```
[ ] docker ps: tất cả container Up
    (hadoop-master, hadoop-worker1, airflow-web, airflow-scheduler,
     hive-metastore, spark-thrift, superset-new)
[ ] http://localhost:9870 — Hadoop HDFS UI: NameNode active
[ ] http://localhost:8088 — YARN UI: 1 Active Node
[ ] http://localhost:8081 — Airflow UI: đăng nhập được, thấy các DAGs của nhóm
[ ] http://localhost:8089 — Superset UI: đăng nhập được (admin / admin123)
[ ] SQL Lab: SHOW TABLES IN gold → thấy đủ 4 bảng
[ ] Dashboard "Supply Chain Analytics Dashboard": Published, 4 charts có data
[ ] DAG superset_dashboard_refresh: import errors = false, 4 tasks đúng flow
```
## Lưu ý cho thành viên

- Bước 8 (khởi tạo database Superset) BẮT BUỘC chạy thủ công lần đầu trên mỗi máy — không tự động khi `docker compose up`
- Mac M1/M2/M3: `docker-compose.yml` có các dòng `platform: linux/amd64`. Nếu dùng Intel/Linux thì xóa các dòng đó trước khi chạy
- spark-thrift bị exit sau khi start: kiểm tra `core-site.xml` và `gcs-connector.jar` đã được copy đúng chưa
