#!/bin/bash

echo "Start YARN..."
start-yarn.sh

echo "Start HDFS..."
start-dfs.sh

echo "Start History Server..."
start-history-server.sh 

echo "Start Container Postgres..."
docker start postgres_container

echo "Running Java processes:"
jps
