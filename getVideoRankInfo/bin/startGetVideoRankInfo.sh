#!/bin/bash
# 执行任务前需要先清理掉上次设置的flag=1的数据
python /data/soft/jobs/neo4jCleanUserFlag.py

# 获取最近一个月的文件目录
file_name=""
for((i=1;i<=30;i++))
do
    file_name+=hdfs://hadoop100:9000/dataCenter/finishedVidInfoNew/`date  -d " $i days ago" +%Y%m%d`,
done

sparkCommonLib="/data/sparkCommLib"
masterUrl="yarn-cluster"
master=`echo ${masterUrl} | awk -F'-' '{print $1}'`
deployMode=`echo ${masterUrl} | awk -F'-' '{print $2}'`

appName="getVideoRankInfo"
hdfsSourceDir=${file_name:0:-1}
boltUrl="bolt://hadoop110:7687"
user="neo4j"
password="admin"

source /etc/profile
spark-submit --master ${master} \
--deploy-mode ${deployMode} \
--driver-memory 1g \
--executor-memory 1g \
--executor-cores 1 \
--num-executors 1 \
--conf spark.memory.fraction=0.8 \
--conf spark.memory.storageFraction=0.3 \
--class getVideoRankInfo \
--jars ${sparkCommonLib}/fastjson-1.2.31.jar,${sparkCommonLib}/neo4j-java-driver-1.2.1.jar \
/data/soft/jobs/getVideoRankInfo-1.0-SNAPSHOT.jar ${masterUrl} ${appName} ${hdfsSourceDir} ${boltUrl} ${user} ${password}

appStatus=`yarn application -appStates FINISHED -list | grep getVideoRankInfo | tail -1 | awk '{print $7}'`
if [ "$appStatus" != "SUCCEEDED" ]
then
    echo "任务执行失败"
    # 发动短信
    echo "计算最近一个月视频评级数据失败[getVideoRankInfo]" 18612345678
fi