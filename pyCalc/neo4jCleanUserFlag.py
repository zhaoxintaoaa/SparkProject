#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Desc: 把用户flag标识为1的重置为0
每周执行一次，当执行spark计算用户直播评级任务之前执行这个脚本
Author: xuwei
Date: 2017/06/24
'''

from neo4j.v1 import GraphDatabase, basic_auth

class Neo4jClean:

    def __init__(self):
        print 'start...'

    def clean(self):

        driver = GraphDatabase.driver("bolt://hadoop110:7687", auth=basic_auth("neo4j", "admin"))
        session = driver.session()
        # 把所有flag=1的user的flag属性重置为0
        session.run("match(a:User) where a.flag=1  set a.flag=0")
        # 只是为了验证上一条命令有没有执行成功
        result = session.run("match(a:User) where a.flag=1  return count(a) as num")
        num = -1
        for record in result:
            num = int(record["num"])
        if num == 0:
            print "success"
        else:
            print "fail",num
        session.close()


if __name__ == '__main__':
    nk = Neo4jClean()
    nk.clean()