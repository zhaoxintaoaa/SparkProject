#!/bin/bash
yestoday=`date -d "-1 days" +"%Y%m%d"`
# 如果输入带参数就使用输入的参数
if [ "x$1" != "x" ]
then
    yestoday=$1
fi

sparkCommonLib="/data/sparkCommLib"
masterUrl="yarn-cluster"
master=`echo ${masterUrl} | awk -F'-' '{print $1}'`
deployMode=`echo ${masterUrl} | awk -F'-' '{print $2}'`

appName="UpdateUserLevel"
boltUrl="bolt://hadoop103:7687"
user="neo4j"
password="admin"
filePath="hdfs://hadoop100:9000/dataCenter/dataReport/cl_level_user/${yestoday}"

# 获取指定日期的时间戳
timestamp=`date --date="${yestoday}" +%s`000

source /etc/profile
spark-submit --master ${master} \
--deploy-mode ${deployMode} \
--driver-memory 1g \
--executor-memory 1g \
--executor-cores 1 \
--num-executors 1 \
--conf spark.memory.fraction=0.8 \
--conf spark.memory.storageFraction=0.3 \
--class UpdateUserLevel \
--jars ${sparkCommonLib}/neo4j-java-driver-1.2.1.jar \

/data/soft/jobs/updateNeo4jUserLevel-1.0-SNAPSHOT.jar ${appName} ${masterUrl} ${boltUrl} ${user} ${password} ${filePath} ${timestamp}


appStatus=`yarn application -appStates FINISHED -list | grep UpdateUserLevel | tail -1 | awk '{print $7}'`
if [ "$appStatus" != "SUCCEEDED" ]
then
    echo "任务执行失败"
    # 发送短信
    echo "更新用户等级信息失败[UpdateUserLevel]" 18612345678
fi