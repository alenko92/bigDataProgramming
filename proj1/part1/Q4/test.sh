#!/bin/sh
../../start.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /temp/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /temp/output/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /temp/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal .././Parking_Violations_Issued_-_Fiscal_Year_2020.csv /temp/input/
/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar \
-files <../Q4/mapper.py> -mapper ../Q4/mapper.py \
-files <../Q4/reducer.py> -reducer ../Q4/reducer.py \
-input /temp/input/* -output /temp/output/
/usr/local/hadoop/bin/hdfs dfs -cat /temp/output/part-00000
/usr/local/hadoop/bin/hdfs dfs -rm -r /temp/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /temp/output/
../../stop.sh
