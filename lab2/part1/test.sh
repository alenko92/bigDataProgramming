#!/bin/bash
source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /lab2/part1/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /lab2/part1/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../shot_logs.csv /lab2/part1/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./kmeans1.py hdfs://$SPARK_MASTER:9000/lab2/part1/input/
