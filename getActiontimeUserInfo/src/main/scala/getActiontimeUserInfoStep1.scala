import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.driver.v1._
import org.neo4j.spark.Neo4j

import scala.collection.mutable.ArrayBuffer

/**
  * 第二版：v2.0
  * 统计最近一个月内的主活用户，
  * 每周执行一次
  * 第一步：首先计算出用户等级在4级以上的所有用户信息
  *
  * Created by xuwei
  */
object getActiontimeUserInfoStep1 {

  def main(args: Array[String]): Unit = {
    var appName = "getActiontimeUserInfoNew"
    var masterUrl = "local"
    var boltUrl = "bolt://127.0.0.1:7687"
    var user = "neo4j"
    var password = "admin"
    var start_timestamp = 0L
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
    params += ("start_timestamp"->start_timestamp)
    params += ("level"->level)

    println("start_timestamp:"+start_timestamp+" level:"+level)
    //获取一月内主活数据 并且用户等级大于4的数据
    //val neo4j: Neo4j = Neo4j(sc).cypher("match (a:User )  return a.uid").params(params)
    val neo4j: Neo4j = Neo4j(sc).cypher("match (a:User )  where a.timestamp >= {start_timestamp} and a.level >= {level} return a.uid").params(params)

    val reRDD = neo4j.loadRowRdd.repartition(6)
    val mapPRDD = reRDD.mapPartitions(ite=>{
      val resultArr = ArrayBuffer[String]()
      ite.foreach(ro => {
        try{
          val uid = ro.getString(0)
          resultArr+=uid
        }catch {
          case ex: Exception => println("异常")
        }
      })
      resultArr.iterator
    })
    // repartition 20是为了后期分20次去循环读数据，避免时间长导致的executor丢失
    mapPRDD.repartition(20).saveAsTextFile(output)

  }

}
