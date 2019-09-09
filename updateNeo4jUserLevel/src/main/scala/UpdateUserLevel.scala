import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.driver.v1.{AuthTokens, GraphDatabase}

/**
  * 更新用户的等级
  * 每天更新一次
  * Created by xuwei
  */
object UpdateUserLevel {

  def main(args: Array[String]): Unit = {
    var appName = "UpdateUserLevel"
    var masterUrl = "local"
    var boltUrl = "bolt://127.0.0.1:7687"
    var user = "neo4j"
    var password = "admin"
    var filePath = "/data/a.log"
    var timestamp = 0L
    if (args.length > 0) {
      appName = args(0)
      masterUrl = args(1)
      boltUrl = args(2)
      user = args(3)
      password = args(4)
      filePath = args(5)
      timestamp = args(6).toLong
    }

    //  获取sc
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(masterUrl)
    // timestamp是shell脚本中获取的昨天的时间戳
    println("timestamp:"+timestamp)
    val sc = new SparkContext(conf)
    // 读取用户等级数据
    // 使用的是从MySQL中读取数据
    val lines = sc.textFile(filePath)
    // 对用户等级数据进行过滤，保留数据有变化的用户信息
    val filterRDD = lines.filter(line => {
      var flag = false
      try{
        val splits = line.split("\t")
        if(splits.length==8){
          //  因为用户等级信息是每天保存一份全量，在这里只需要过滤出来昨天等级发生过变化的数据即可
          val update_time = dateUtil.dateTime2unixTime(splits(5),"yyyy-MM-dd HH:mm:ss").toLong
          if(update_time >= timestamp){
            flag = true
          }
        }
      }catch {
        case ex: Exception => println("数据异常："+ex.getCause)
      }
      flag
    })

    filterRDD.foreachPartition(ite => {
      val driver = GraphDatabase.driver( boltUrl, AuthTokens.basic( user, password ) )
      val session = driver.session()
      ite.foreach(line => {
        //6       740591571128680448      380     3       2016-07-05 17:48:22     2017-02-10 11:43:18     0       18
        try{
          val splits = line.split("\t")
          // 判断需要的数据 ID 等级 不为空
          if(splits.length==8 && !splits(1).trim.eq("") && !splits(3).trim.eq("")){
            // 为相应ID 的主播 更新 等级信息
            session.run("MERGE(user:User {uid:\""+splits(1).trim+"\"}) SET user.level = "+splits(3).trim+"")//添加等级
          }else{
            println("数据不符合格式:"+line)
          }
        }catch {
          case ex: Exception => println("数据异常："+ex.getCause)
        }finally {
        }
      })
      session.close()
    })



  }

}
