#!/bin/bash

echo "Stop YARN..."
stop-yarn.sh

echo "Stop HDFS..."
stop-dfs.sh

echo "Stop History Server..."
stop-history-server.sh 

echo "Running Java processes::"
jps

