#!/bin/bash
set -e

sleep 10

if [ ! -d "/data/hdfs/namenode/current" ]; then
    su - ${USER_NAME} -c "$HADOOP_HOME/bin/hdfs namenode -format -force -nonInteractive"
fi

su - ${USER_NAME} -c "$HADOOP_HOME/bin/hdfs --daemon start namenode"

su - ${USER_NAME} -c "$HADOOP_HOME/bin/yarn --daemon start resourcemanager"

sleep 10

su - ${USER_NAME} -c "$HADOOP_HOME/bin/hdfs dfs -mkdir -p /spark-logs" || true
su - ${USER_NAME} -c "$HADOOP_HOME/bin/hdfs dfs -chmod 777 /spark-logs" || true

su - ${USER_NAME} -c "$HADOOP_HOME/bin/hdfs dfs -mkdir -p /tmp" || true
su - ${USER_NAME} -c "$HADOOP_HOME/bin/hdfs dfs -chmod 1777 /tmp" || true

tail -f /dev/null