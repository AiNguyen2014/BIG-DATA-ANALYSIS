#!/bin/bash
set -e

mkdir -p /tmp/hadoop-pids
mkdir -p /tmp/yarn/local
mkdir -p /tmp/yarn/logs
mkdir -p /opt/hadoop/logs
mkdir -p /data/hdfs/namenode
mkdir -p /data/hdfs/datanode

chown -R ${USER_NAME}:${USER_NAME} \
    /tmp/hadoop-pids \
    /tmp/yarn \
    /opt/hadoop/logs \
    /data

service ssh start

if [ -f /opt/keys/key.json ]; then
    export GOOGLE_APPLICATION_CREDENTIALS=/opt/keys/key.json
fi

case "$NODE_TYPE" in
    master)
        /scripts/start-master.sh
        ;;
    worker)
        /scripts/start-worker.sh
        ;;
    *)
        tail -f /dev/null
        ;;
esac