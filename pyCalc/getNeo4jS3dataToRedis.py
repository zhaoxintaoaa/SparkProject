#/usr/bin/python
#-*- coding: UTF-8 -*-

'''
Desc: 读取指定s3的数据写入到redis中
Author:xuwei
'''
import redis
import traceback
import sys


def get_file_to_redis():
    try:
        # 获取redis链接
        redisConnection = redis.Redis("192.168.100.170",6379,0
                                  ,socket_timeout=1,socket_connect_timeout=5)
        pipe = redisConnection.pipeline(transaction=False)
        num = 0

        with open(filename,'r') as fp:
            lines = fp.readlines()
            for line in lines:
                splits = line.strip('\n').split('\t')
                if len(splits) == 2:
                    tmp_splits = splits[1].split(',')
                    if len(tmp_splits) > 1:
                        rediskey_end = rediskey+str(splits[0])
                        pipe.delete(rediskey_end)
                        for i in tmp_splits:
                            pipe.rpush(rediskey_end,i)
                        pipe.expire(rediskey_end,30*24*60*60)
                        num += 1
                        if num % 5000 == 0:
                            pipe.execute()
                            pipe = redisConnection.pipeline(transaction=False)
                else:
                    print "数据异常！！！"
            pipe.execute()
    except Exception, e:
        print e
        print traceback.print_exc()
if __name__ == "__main__":
    filename = sys.argv[1]
    rediskey = "cache:guide:recommend:friends:"
    get_file_to_redis()
