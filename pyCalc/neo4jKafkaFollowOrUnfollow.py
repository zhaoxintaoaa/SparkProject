#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Desc: 实时从kafka中获取关注和取关数据，更新neo4j中的用户关系
Author: xuwei
注意：这个python脚本启动之后，可以使用linux中的supervisor组件对其进行监控
'''

from neo4j.v1 import GraphDatabase, basic_auth
from pykafka import KafkaClient
from pykafka.balancedconsumer import OffsetType
import json,traceback


class Neo4jKafka:

    def __init__(self):
        print 'start...'

    def neo4j_operation(self):
        # 获取neo4j链接
        driver = GraphDatabase.driver("bolt://hadoop110:7687", auth=basic_auth("neo4j", "admin"))
        session = driver.session()

        # 指定kafka的配置信息
        hosts = "hadoop110:9092"
        topic_name = 'serverLog2DataBase_r2p6'
        while True:
            try:
                # 获取kafka链接
                client = KafkaClient(hosts=hosts)
                topic = client.topics[topic_name]
                # 开启消费者，
                # 指定auto_offset_reset为EARLIEST，表示第一次启动的时候从头消费数据，
                # kafka的信息会保存两天，为了保证不丢失数据可能会消费重复一部分数据 但是对结果不会有影响因为使用的是merge每次都会先检查之后执行
                # auto_commit_enable为true：表示会提交offset，以后重启这个程序的时候就会根据offset偏移量消费
                simple_consumer = topic.get_simple_consumer(auto_offset_reset=OffsetType.EARLIEST,consumer_group="neo4jConsumerNew8",auto_commit_enable=True)

                for msg in simple_consumer:
                    if msg.value:
                        # 把json字符串转为json对象
                        msg_json = json.loads(msg.value)
                        '''
                        获取的json数据格式
                        {"followeruid":"875451267710189568","followuid":"792341372689973248","timestamp":1498198304,"type":"user_relation_follow"}
                        '''
                        # 获取字段中的type字段，只解析follow(关注)和unfollow(取消关注)的数据
                        follow_type = msg_json.get("type")
                        if follow_type == 'user_relation_follow' or follow_type == 'user_relation_unfollow':
                            self.follow_or_unfollow(msg_json, session, follow_type)

            except Exception,e:
                print e
                print traceback.print_exc()
        # session.close()

    def follow_or_unfollow(self, msg_json, session, follow_type):
        '''
        操作neo4j,执行添加关注和取消关注的动作
        :param msg_json:
        :param session:
        :param follow_type:
        :return:
        '''
        # 获取关注者UID
        followeruid = msg_json.get('followeruid')
        # 获取被关注者UID
        followuid = msg_json.get('followuid')

        if follow_type == 'user_relation_follow':
            # 添加关注
            if followeruid and followuid:
                self.add_user_replationship(session, str(followeruid), str(followuid))
        else:
            # 取消关注
            if followeruid and followuid:
                self.del_user_replationship(session, str(followeruid), str(followuid))

    # 添加关注
    def add_user_replationship(self,session, followeruid, followuid):
        # 这里会调用add_replationship函数
        session.write_transaction(self.add_replationship, followeruid, followuid)
        print "add_follow:{0}-->{1}".format(followeruid,followuid)

    @staticmethod
    def add_replationship(tx, followeruid, followuid):
        #  检测已经变化的关注者或者被关注者是否存在 已存在则不变 不存在则创建
        tx.run("MERGE (:User { uid: $followeruid})",followeruid=followeruid)
        tx.run("MERGE (:User { uid: $followuid})",followuid=followuid)
        #  检查完以后为两者添加follow关系  查询两个节点 -> 添加follow关系
        tx.run("MATCH(a:User {uid: $followeruid}),(b:User {uid: $followuid}) MERGE (a)-[:follow]->(b)",followeruid=followeruid,followuid=followuid)

    # 取消关注
    def del_user_replationship(self,session, followeruid, followuid):
        # 这里会执行del_replationship
        session.write_transaction(self.del_replationship, followeruid, followuid)
        print "del_follow:{0}-->{1}".format(followeruid,followuid)

    @staticmethod
    def del_replationship(tx, followeruid, followuid):
        # 不需要检查两个节点的存在与否 因为是取消关注所以肯定已经存在在Neo4j数据库中
        tx.run("MATCH (:User { uid: $followeruid})-[r:follow]->(:User { uid: $followuid}) DELETE r ",followeruid=followeruid,followuid=followuid)

if __name__ == '__main__':
    nk = Neo4jKafka()
    nk.neo4j_operation()