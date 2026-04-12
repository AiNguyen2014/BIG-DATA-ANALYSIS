#!/bin/bash
set -e

echo "Waiting for HDFS..."

until sudo -E -u ${USER_NAME} $HADOOP_HOME/bin/hdfs dfs -ls / >/dev/null 2>&1; do
  echo "HDFS not ready yet..."
  sleep 5
done

echo "HDFS ready"

echo "Starting DataNode..."
sudo -E -u ${USER_NAME} $HADOOP_HOME/bin/hdfs --daemon start datanode

echo "Starting NodeManager..."
sudo -E -u ${USER_NAME} $HADOOP_HOME/bin/yarn --daemon start nodemanager

sleep 5
jps

tail -f /dev/null