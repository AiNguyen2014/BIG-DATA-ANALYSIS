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
|___|__bronze_silver_gold_dag.py
|___|__silver_processing.py
|___|__silver_to_gold_job.py
|___|__superset_dashboard_refresh_dag.py
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
├── sql_query/
|   |__ analysis_query.sql
├── logs 
├── .env
├── .env.example
├── .gitignore
├── docker-compose.yml
└── README.md

```      