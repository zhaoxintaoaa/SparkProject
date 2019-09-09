import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.driver.v1.{AuthTokens, GraphDatabase}

/**
  * 读取主活数据，给neo4j中的数据添加最后活跃字段 timestamp
  * 每天更新一次
  * Created by xuwei
  */
object UpdateUserActiontime {

  def main(args: Array[String]): Unit = {
    var appName = "UpdateUserActiontime"
    var masterUrl = "local"
    var boltUrl = "bolt://192.168.111.100:7687"
    var user = "neo4j"
    var password = "admin"
    var filePath = "hdfs://hadoop100:9000/dataCenter/action1/20190906"
    if (args.length == 6) {
      appName = args(0)
      masterUrl = args(1)
      boltUrl = args(2)
      user = args(3)
      password = args(4)
      filePath = args(5)
    }

    // 获取sc
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(masterUrl)
    val sc = new SparkContext(conf)

    //  读取hdfs中的数据
    val lines = sc.textFile(filePath)

    lines.foreachPartition(ite => {
      //  获取neo4j链接
      val driver = GraphDatabase.driver( boltUrl, AuthTokens.basic( user, password ) )
      val session = driver.session()

      ite.foreach(line => {
        //{"uid":"876698695604109312","mcc":"452","countryCode":"VN","ver":"3.6.41","UnixtimeStamp":"1498300271000","ip":"171.247.0.154, 54.239.129.77"}
        try{
          //  从json字符串中解析对应的uid和UnixtimeStamp
          val lineObj = JSON.parseObject(line)
          val uid = lineObj.getString("uid")
          val timeStamp = lineObj.getString("UnixtimeStamp")
          //  执行neo4j
          session.run("MERGE(user:User {uid:\""+uid+"\"}) SET user.timestamp = "+timeStamp+"")//添加活跃时间
        }catch {
          case ex: Exception => ex.printStackTrace()
        }finally {
        }
      })
      session.close()
    })

  }

}
