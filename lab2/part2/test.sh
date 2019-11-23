#!/bin/bash
source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /lab2/part2/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /lab2/part2/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../parkingSample.csv /lab2/part2/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./kmeans.py hdfs://$SPARK_MASTER:9000/lab2/part2/input/
