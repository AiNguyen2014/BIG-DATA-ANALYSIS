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