import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.driver.v1.{AuthTokens, GraphDatabase}


/**
  * 解析finishedvideinfo中的主播id和视频rating等级值
  * {"id":"14982333179038114966","uid":"815261283682615296","nickname":"Fre@kyziahiar"gold":0,"watchnumpv":35,
  * "watchnumuv":19,"hosts":40,"nofollower":17,"looktime":394,"smlook":4,"follower":2,"gifter":0,"length":279,
  * "area":"A_US","rating":"D","exp":0,"timestamp":1498233628,"type":"video_rating"}
  *
  * 把最近几次视频评级在3B+或2A+的数据，给满足条件的数据在noe4j中设置flag=1
  * 每周执行一次
  * 注意：在执行之前需要先执行程序把flag=1的重置为0
  *
  */
object getVideoRankInfo {

  def main(args: Array[String]): Unit = {
    var masterUrl = "local"
    var appName = "getVideoRankInfo"
    var hdfsSourceDir = "/data/b.log"
    var boltUrl = "bolt://127.0.0.1:7687"
    var user = "neo4j"
    var password = "admin"
    if (args.length >0 ) {
      masterUrl = args(0)
      appName = args(1)
      hdfsSourceDir = args(2)
      boltUrl = args(3)
      user = args(4)
      password = args(5)
    }
    //  获取sc
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    val sc = new SparkContext(conf)
    //  加载数据
    val lines = sc.textFile(hdfsSourceDir).distinct()
    println("文件路径："+hdfsSourceDir)

    // 解析数据中的uid，rating，timestamp
    val tupleRDD = lines.map(line =>{
      try {
        val obj = JSON.parseObject(line)
        val uid = obj.getString("uid")
        val rank  = obj.getString("rating")
        val timestamp = obj.getString("timestamp")
        (uid,rank,timestamp)
      }catch {
        case ex: Exception =>println("json数据解析失败！"+line)
          ("0","0","0")
      }
    }).filter(_._2 != "0")//过滤掉解析失败的值

    //获取用户最近3场直播的评级信息
    //根据ID分组 在根据时间戳排序 获取前三个
    val top3RDD = tupleRDD.groupBy(_._1).map(group=>{
      //reverse就是将顺序反转  由从小到大 反转成从大到小
      val topK=group._2.toList.sortBy(_._3).reverse.take(3).mkString("\t")
      (group._1,topK)
    })


    //过滤出来满足3场B+的数据 把uid保存下来 [等级>=15]
    //top3BRDD  tup  splits  就是 topK最近三场直播信息
    val top3BRDD = top3RDD.filter(tup=>{
      var flag = false
      val splits = tup._2.split("\t")
      if(splits.length==3){
        //  tmp_str :  A,A,A
        val tmp_str = splits(0).split(",")(1)+","+ splits(1).split(",")(1)+","+splits(2).split(",")(1)
        if(!tmp_str.contains("C") && !tmp_str.contains("D")) {
          flag = true
        }
      }
      flag
    })

    //TODO  还需要对计算出来的数据过滤掉封禁的主播
    //  把满足三场为B+的数据更新到neo4j中，增加个字段flag，属性值置位1，flag=1 我们认为是满足条件的数据，允许推荐给用户
    top3BRDD.foreachPartition(ite => {
      val driver = GraphDatabase.driver( boltUrl, AuthTokens.basic( user, password ))
      val session = driver.session()
      ite.foreach(tup=>{
        //判断等级是否达到15
        session.run("match(a:User{uid:\""+tup._1+"\"}) where a.level >=15  set a.flag=1")
      })
      session.close()
    })

    //过滤出来满足2场A+的数据，把uid保存下来 [等级>=4]
    val top2ARDD = top3RDD.filter(tup=>{
      var flag = false
      val splits = tup._2.split("\t")
      if(splits.length==3){
        val tmp_str = splits(0).split(",")(1)+","+ splits(1).split(",")(1)
        if(!tmp_str.contains("B") && !tmp_str.contains("C") && !tmp_str.contains("D")) {
          flag = true
        }
      }
      flag
    })
    //TODO  还需要对计算出来的数据过滤掉封禁的主播
    top2ARDD.foreachPartition(ite => {
      val driver = GraphDatabase.driver( boltUrl, AuthTokens.basic( user, password ))
      val session = driver.session()
      ite.foreach(tup=>{
        session.run("match(a:User{uid:\""+tup._1+"\"}) where a.level >=4  set a.flag=1")
      })
      session.close()
    })

  }

}
