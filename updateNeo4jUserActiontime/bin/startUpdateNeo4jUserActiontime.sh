#!/bin/bash

yestoday=`date -d "-1 days" +"%Y%m%d"`

# 判断是否意见传递参数传递了的话就使用传递的参数作为yestoday
if [ "x$1" != "x" ]
then
    yestoday=$1
fi
# 获取master  以及  deployMode
sparkCommonLib="/data/sparkCommLib"
masterUrl="yarn-cluster"
master=`echo ${masterUrl} | awk -F'-' '{print $1}'`
deployMode=`echo ${masterUrl} | awk -F'-' '{print $2}'`

# 定义任务需要的参数
appName="UpdateUserActiontime"
boltUrl="bolt://hadoop103:7687"
user="neo4j"
password="admin"
filePath="hdfs://hadoop100:9000/dataCenter/action1/${yestoday}"

# 强制在执行spark脚本之前，先重新加载环境变量
source /etc/profile
spark-submit --master ${master} \
--deploy-mode ${deployMode} \
--driver-memory 1g \
--executor-memory 1g \
--executor-cores 1 \
--num-executors 1 \
--conf spark.memory.fraction=0.8 \
--conf spark.memory.storageFraction=0.3 \
--class UpdateUserActiontime \
--jars ${sparkCommonLib}/fastjson-1.2.31.jar,${sparkCommonLib}/neo4j-java-driver-1.2.1.jar \

#  执行spark脚本
/data/soft/jobs/updateNeo4jUserActiontime-1.0-SNAPSHOT.jar ${appName} ${masterUrl} ${boltUrl} ${user} ${password} ${filePath}
#  获取SUCCEEDED字段在进行判断  失败的话就不是SUCCEEDED了
appStatus=`yarn application -appStates FINISHED -list | grep UpdateUserActiontime | tail -1 | awk '{print $7}'`
if [ "$appStatus" != "SUCCEEDED" ]
then
    echo "任务执行失败"
    # 发送短信
    echo "更新用户最新活跃时间失败[UpdateUserActiontime]" 18612345678
fi