import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.driver.v1._
import org.neo4j.spark.Neo4j

import scala.collection.mutable.ArrayBuffer

/**
  * 第二版：v2.0
  * 统计最近一个月内的主活用户，保证此用户的粉丝列表中关注重合度要>2  并且用户等级在4级以上
  * 每周执行一次
  * Created by xuwei on 17/6/24.
  */
object getActiontimeUserInfoStep2 {

  def main(args: Array[String]): Unit = {
    var appName = "getActiontimeUserInfoNew"
    var masterUrl = "local"
    var boltUrl = "bolt://127.0.0.1:7687"
    var user = "neo4j"
    var password = "admin"
    var start_timestamp = 0L
    var input = ""
    var output = "/data/getActiontimeUserInfoNew"
    var num = 0
    var level = 4
    if (args.length>0) {
      appName = args(0)
      masterUrl = args(1)
      boltUrl = args(2)
      user = args(3)
      password = args(4)
      start_timestamp = args(5).toLong
      input = args(6)
      output = args(7)
      num = args(8).toInt
      level = args(9).toInt
    }
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(masterUrl)
      .set("spark.driver.allowMultipleContexts","true")
      .set("spark.neo4j.bolt.url", boltUrl)
      .set("spark.neo4j.bolt.user",user)
      .set("spark.neo4j.bolt.password",password)

    val sc = new SparkContext(conf)

    println("start_timestamp:"+start_timestamp+" level:"+level)

    val linesRDD = sc.textFile(input).repartition(6)


    // 过滤出粉丝关注重合度>2的数据，并且对关注重合度倒序排列 filter之后的数据格式是：  主播id,推荐的主播id,关注重合度
    val mapPRDD = linesRDD.mapPartitions(ite=>{
      val config = Config
        .build()
        .withTrustStrategy(Config.TrustStrategy.trustAllCertificates())
        .toConfig
      val driver = GraphDatabase.driver( boltUrl, AuthTokens.basic( user, password ),config)
      val session = driver.session()

      val resultArr = ArrayBuffer[String]()
      ite.foreach(line => {
        try{
          val uid = line.replace("\n","")
          //println("process:"+uid)
          //val result = session.run("match (a:User {uid:\""+uid+"\"}) <-[:follow]- (b:User) -[:follow]-> (c:User)  return a.uid as auid,c.uid as cuid,count(c.uid) as sum")
          val result = session.run("match (a:User {uid:\""+uid+"\"}) <-[:follow]- (b:User) -[:follow]-> (c:User) where  b.timestamp >= "+start_timestamp+"  and c.timestamp >= "+start_timestamp+"  and c.level >="+level+" and c.flag = 1 return a.uid as auid,c.uid as cuid,count(c.uid) as sum order by sum desc limit 30")
          while(result.hasNext){
            val record = result.next()
            val sum = record.get("sum").asInt()
            if(sum>num){
              resultArr+=record.get("auid").asString()+"\t"+record.get("cuid").asString()
            }
          }
        }catch {
          case ex: Exception => println("异常")
        }
      })
      session.close()
      resultArr.iterator
    }).persist(StorageLevel.MEMORY_AND_DISK)//将数据缓存起来  持久化
    val map1RDD = mapPRDD.map(line => {
      val splits = line.split("\t")
      (splits(0),splits(1))
    })
    val reduceRDD = map1RDD.reduceByKey((v1,v2)=>{
      v1+","+v2
    })

    reduceRDD.map(tup=>{
      tup._1+"\t"+tup._2

    }).repartition(20).saveAsTextFile(output)//分20分是为了避免一个executor太长导致运行时间过长而失败
    //将分为6分六个进程的数据 每一份在分成为20个块每次读取的时候都从hdfs中读取一个并把处理结果持久化到内存中以后再将结果保存到hdfs中

  }

}
