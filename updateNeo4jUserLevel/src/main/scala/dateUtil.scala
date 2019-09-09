import java.text.SimpleDateFormat
import java.util.{Calendar, Date}


/**
  * 日期工具类
  */


object dateUtil {
  //获得当前时间 格式自己定义 yyyy-MM-dd
  def getNowDate(dataFormat:String):String= {
    var nowTime = ""
    try{
      var now = new Date()
      var dateFormat = new SimpleDateFormat(dataFormat)
      nowTime = dateFormat.format(now)
    }catch {
      case ex: Exception => ex.printStackTrace()
    }
    nowTime
  }
  //获得昨天日期 yyyy-MM-dd
  def getYesterday():String={
    var  dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal=Calendar.getInstance()
    cal.add(Calendar.DATE,-1)
    var yesterday=dateFormat.format(cal.getTime())
    yesterday
  }
  //获得本周一的日期 yyyy-MM-dd
  def getNowWeekStart():String={
    var period:String=""
    var cal =Calendar.getInstance()
    var df = new SimpleDateFormat("yyyy-MM-dd")
    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    period=df.format(cal.getTime())
    period
  }
  //获得本周末 yyyy-MM-dd
  def getNowWeekEnd():String={
    var period:String=""
    var cal =Calendar.getInstance()
    var df = new SimpleDateFormat("yyyy-MM-dd")
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY)//这种输出的是上个星期周日的日期
    cal.add(Calendar.WEEK_OF_YEAR, 1)// 增加一个星期
    period=df.format(cal.getTime())
    period
  }
  //获得本月第一天
  def getNowMonthStart():String={
    var period:String=""
    var cal =Calendar.getInstance()
    var df = new SimpleDateFormat("yyyy-MM-dd")
    cal.set(Calendar.DATE, 1)
    period=df.format(cal.getTime())
    period
  }
  //本月最后一天
  def getNowMonthEnd():String={
    var period:String=""
    var cal =Calendar.getInstance()
    var df = new SimpleDateFormat("yyyy-MM-dd")
    cal.set(Calendar.DATE, 1)
    cal.roll(Calendar.DATE,-1)
    period=df.format(cal.getTime())//本月最后一天
    period
  }
  //将时间戳转化为日期  参数1 时间戳 参数2 日期格式
  def unixTime2dateTime(time:String,dateFormat:String):String={
    var date = ""
    try{
      var sdf:SimpleDateFormat = new SimpleDateFormat(dateFormat)
      if(time.length > 11){
        date= sdf.format(new Date((time.toLong)))
      }else{
        date= sdf.format(new Date((time.toLong*1000l)))
      }
    }catch {
      case ex: Exception => ex.printStackTrace()
    }
    date
  }
  //dateTime 转化时间戳 参数1 时间  参数2 时间格式
  def dateTime2unixTime(dateTime:String,dateFormat: String): String ={
    var unixTime = ""
    try{
        val format = new SimpleDateFormat(dateFormat)
        val dt = format.parse(dateTime)
        unixTime = dt.getTime().toString()
    }catch{
      case ex: Exception => ex.printStackTrace()
    }
    unixTime
  }

  def getNowTime():String = {
    var now: Date = new Date()
    now.getTime().toString
  }

  def getDateBefore(days:Int,dateFormat:String):String ={
    var date = ""
    try{
      var cal =Calendar.getInstance()
      var df = new SimpleDateFormat(dateFormat)
      var now: Date = new Date()
      cal.add(Calendar.DAY_OF_YEAR, (0-days))
      date=df.format(cal.getTime())
    }catch {
      case ex:Exception => ex.printStackTrace()
    }
      date
  }
  def getDateAfter(days:Int,dateFormat:String):String ={
    var date = ""
    try{
      var cal =Calendar.getInstance()
      var df = new SimpleDateFormat(dateFormat)
      var now: Date = new Date()
      cal.add(Calendar.DAY_OF_YEAR, days)
      date=df.format(cal.getTime())
    }catch {
      case ex:Exception => ex.printStackTrace()
    }
    date
  }

  def main(args:Array[String]){

    println("现在时间：" + getNowDate("yyyyMMddHHmm"))
    var nowTime = getNowTime()
    println("Now time:" + nowTime)
    var nowDate = unixTime2dateTime(nowTime,"yyyy-MM-dd HH:mm:ss")
    println("Now date:" + nowDate)
    println("时间转换:" + dateTime2unixTime("2016-07-19 23:12:00","yyyy-MM-dd HH:mm:ss"))
    println(dateUtil.unixTime2dateTime(dateUtil.getNowTime(),"yyyyMMddHH").toLong - 1)
    var lastHourunixTime = dateUtil.dateTime2unixTime((dateUtil.unixTime2dateTime((dateUtil.getNowTime().toLong - 3600).toString,"yyyyMMddHH")).toString,"yyyyMMddHH")
    println("+++++:" + lastHourunixTime)
    println("getDateBefore:",getDateBefore(2,"yyyy-MM-dd"))
    println("getDateAfter:",getDateAfter(2,"yyyy-MM-dd"))

    println("+++"+60*60*24)
    println("今天开始的unix时间:"+dateTime2unixTime(getNowDate("yyyyMMdd"),"yyyyMMdd")+" 昨天开始的unix时间:"+dateTime2unixTime(getDateBefore(1,"yyyyMMdd"),"yyyyMMdd"))

  }
}

