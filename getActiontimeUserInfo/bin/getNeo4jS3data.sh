#!/bin/bash
s3_path=$1
file_name=$2
# 把hdfs上的数据重定向到本地磁盘文件中
hdfs dfs -cat ${s3_path} > ${file_name}

python /data/soft/jobs/getNeo4jS3dataToRedis.py ${file_name}