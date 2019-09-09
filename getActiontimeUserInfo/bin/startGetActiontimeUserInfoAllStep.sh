#!/bin/bash
# 执行第一步
sh -x ./startGetActiontimeUserInfoStep1.sh

# 执行第二步
weekday=`date +%w`
days=$[$weekday+6]
time_path=`date -d "-${days} days" +"%Y%m%d"`
step1_output="hdfs://hadoop100:9000/dataCenter/getActiontimeUserInfo/step1/${time_path}"


step2_output="hdfs://hadoop100:9000/dataCenter/getActiontimeUserInfo/step2/${time_path}/"
hdfs dfs -mkdir -p ${step2_output}
# 获取第一步输出的所有文件
file_name_arr=(`hdfs dfs -ls ${step1_output}  | grep -v "bak" | grep -v "_SUCCESS" | grep -v "Found" | awk '{print $8}'`)
for filename in ${file_name_arr[@]}
do
        # 执行spark job
        sh -x ./startGetActiontimeUserInfoStep2.sh ${filename} ${step2_output}${filename:0-10:10}


        # 验证spark job是否执行结束(验证输出目录中是否有_SUCCESS文件)
        hdfs dfs -ls ${step2_output}${filename:0-10:10}/_SUCCESS >/dev/null 2>&1
        if [ $? -ne 0 ]
        then
                echo "本次任务执行失败...${filename}"
                # 发短信
                echo  "三度任务数据计算失败->${filename}[getActiontimeUserInfo]" 18612345678
                continue
        else
            # 给文件重命名 表示已经处理过
            hdfs dfs -mv ${filename} ${filename}.bak
        fi
done


sh getNeo4jS3data.sh "${step2_output}*/*" getActiontimeUserInfo.data

message="【失败】三度关系数据入库redis"
if [ $? -eq 0 ]
then
    message="【成功】三度关系数据入库redis"
fi
# 发短息
echo ${message} 18612345678