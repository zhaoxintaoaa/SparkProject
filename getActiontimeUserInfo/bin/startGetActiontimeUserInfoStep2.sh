#!/bin/bash
sparkCommonLib="/data/sparkCommLib"
masterUrl="yarn-cluster"
master=`echo ${masterUrl} | awk -F'-' '{print $1}'`
deployMode=`echo ${masterUrl} | awk -F'-' '{print $2}'`

appName="getActiontimeUserInfo"
boltUrl="bolt://hadoop110:7687"
user="neo4j"
password="admin"
weekday=`date +%w`
days=$[$weekday+6]
start_tmp_tme=`date -d "-${days} days" +"%Y-%m-%d 00:00:00"`
end_tmp_tme=`date -d "-${weekday} days" +"%Y-%m-%d 23:59:59"`
start_timestamp=`date --date="$start_tmp_tme"  +%s`000
end_timestamp=`date --date="$end_tmp_tme"  +%s`000
time_path=`date -d "-${days} days" +"%Y%m%d"`
#input="hdfs://hadoop100:9000/dataCenter/getActiontimeUserInfo/step1/${time_path}"
input=$1
#output="hdfs://hadoop100:9000/dataCenter/getActiontimeUserInfo/step2/${time_path}"
output=$2
#指定过滤标准
num=2
level=4
if [ "x${output}" = "x" ]
then
    echo "没有指定输出目录！！！"
    exit
fi
hdfs dfs -rm -r ${output}

source /etc/profile
spark-submit --master ${master} \
--deploy-mode ${deployMode} \
--driver-memory 1g \
--executor-memory 1g \
--executor-cores 1 \
--num-executors 1 \
--conf spark.memory.fraction=0.8 \
--conf spark.memory.storageFraction=0.3 \
--class getActiontimeUserInfoStep2 \
--jars ${sparkCommonLib}/fastjson-1.2.31.jar,${sparkCommonLib}/neo4j-spark-connector-2.0.0-M2-custorm.jar,${sparkCommonLib}/neo4j-java-driver-1.2.1.jar \
/data/soft/jobs/getActiontimeUserInfo-1.0-SNAPSHOT.jar  ${appName} ${masterUrl} ${boltUrl} ${user} ${password} ${start_timestamp} ${input} ${output} ${num} ${level}
