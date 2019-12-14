#!/bin/bash
source ../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /proj2/part1/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /proj2/part1/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../train.csv /proj2/part1/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../test.csv /proj2/part1/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./part1.py hdfs://$SPARK_MASTER:9000/proj2/part1/input/
