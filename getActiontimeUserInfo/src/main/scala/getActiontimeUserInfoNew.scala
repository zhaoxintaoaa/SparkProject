import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.driver.v1._
import org.neo4j.spark.Neo4j

import scala.collection.mutable.ArrayBuffer

/**
  * 第一版：V1.0
  * 统计最近一个月内的主活用户，保证此用户的粉丝列表中关注重合度要>2  并且用户等级在4级以上
  * 每周执行一次
  * Created by xuwei
  */
object getActiontimeUserInfoNew {

  def main(args: Array[String]): Unit = {
    var appName = "getActiontimeUserInfoNew"
    var masterUrl = "local"
    var boltUrl = "bolt://hadoop100:7687"
    var user = "neo4j"
    var password = "admin"
    var timestamp = 0L
    var output = "/data/getActiontimeUserInfoNew"
    var num = 0
    var level = 4
    if (args.length == 9) {
      appName = args(0)
      masterUrl = args(1)
      boltUrl = args(2)
      user = args(3)
      password = args(4)
      timestamp = args(5).toLong
      output = args(6)
      num = args(7).toInt
      level = args(8).toInt
    }
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(masterUrl)
      .set("spark.driver.allowMultipleContexts","true")
      .set("spark.neo4j.bolt.url", boltUrl)
      .set("spark.neo4j.bolt.user",user)
      .set("spark.neo4j.bolt.password",password)

    val sc = new SparkContext(conf)

    var params = Map[String,Long]()
    params += ("timestamp"->timestamp)
    params += ("level"->level)

    println("timestamp:"+timestamp+" level:"+level)
    //获取一月内主活数据 并且主播等级大于4的数据
    //val neo4j: Neo4j = Neo4j(sc).cypher("match (a:User )  return a.uid").params(params)
    val neo4j: Neo4j = Neo4j(sc).cypher("match (a:User )  where a.timestamp >= {timestamp} and a.level >= {level}  return a.uid").params(params)

    //  repartition 这里的repartition是为为了把数据分为6份，这样下面的mappartition在执行的时候就有6个线程，这6个线程并行查询neo4j数据库
    val reRDD = neo4j.loadRowRdd.repartition(6)
    // 过滤出粉丝关注重合度>2的数据，并且对关注重合度倒序排列 filter之后的数据格式是：  主播id,推荐的主播id,关注重合度
    val mapPRDD = reRDD.mapPartitions(ite=>{
      //  指定neo4j的参数信息
      val config = Config
        .build()
        .withTrustStrategy(Config.TrustStrategy.trustAllCertificates())
        .toConfig
      val driver = GraphDatabase.driver( boltUrl, AuthTokens.basic( user, password ),config)
      val session = driver.session()

      val resultArr = ArrayBuffer[String]()
      ite.foreach(ro => {
        try{
          val uid = ro.getString(0)
          //println("process:"+uid)
          //val result = session.run("match (a:User {uid:\""+uid+"\"}) <-[:follow]- (b:User) -[:follow]-> (c:User)  return a.uid as auid,c.uid as cuid,count(c.uid) as sum")
          //  获取某一个用户的三度关系（主播的二度关系）,这个任务主要消耗的时间都在这里了
          //  取出重合度前三十位
          val result = session.run("match (a:User {uid:\""+uid+"\"}) <-[:follow]- (b:User) -[:follow]-> (c:User) where  b.timestamp >= "+timestamp+" and c.timestamp >= "+timestamp+" and c.level >="+level+" and c.flag = 1 return a.uid as auid,c.uid as cuid,count(c.uid) as sum order by sum desc limit 30")
          while(result.hasNext){
            val record = result.next()
            val sum = record.get("sum").asInt()
            //指定最小重合度
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
    }).persist(StorageLevel.MEMORY_AND_DISK)//把RDD数据缓存起来

    val map1RDD = mapPRDD.map(line => {
      val splits = line.split("\t")
      (splits(0),splits(1))
    })

    val reduceRDD = map1RDD.reduceByKey((v1,v2)=>{
      v1+","+v2
    })

    //  1001  1002,1003,1004
    reduceRDD.map(tup=>{
      tup._1+"\t"+tup._2
    }).repartition(1).saveAsTextFile(output)

  }

}
