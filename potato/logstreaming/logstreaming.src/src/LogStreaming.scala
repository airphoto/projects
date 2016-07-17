import java.io.{File, FileInputStream}
import java.sql.{PreparedStatement, Connection}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkFiles, SparkContext, SparkConf}
import org.apache.spark.sql.{ Row, DataFrame, SQLContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object LogStreaming {
  def main(args: Array[String]) {
    if (args.length < 6) {
      println("LogStreaming <zkQuorum> <group> <topics> <numThreads> <secondsToCount> <mysqlTable>")
      System.exit(0)
    }
    Logger.getRootLogger.setLevel(Level.WARN)

    val Array(zkQuorum, group, topics, numThreads, secondsToCount,mysqlTable) = args

    val sparkConf = new SparkConf().setAppName("formatlog Streaming")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .set("spark.streaming.unpersist","true")
      .set("spark.default.parallelism","10")
      .set("spark.shuffle.consolidateFiles","true")
      .set("spark.streaming.backpressure.enabled","true")
      .set("spark.streaming.receiver.writeAheadLog.enable","true")
      .set("spark.speculation","true")
      .set("spark.streaming.stopGracefullyOnShutdown","true")
      .set("spark.streaming.receiver.maxRate","3000")
      .set("spark.executor.extraJavaOptions","-XX:+UseConcMarkSweepGC -Dlog4j.configuration=log4j-eir.properties")

    val ssc = new StreamingContext(sparkConf, Seconds(secondsToCount.toLong))
    ssc.sparkContext.addFile("dbcp.properties")
    val in = new FileInputStream(new File(SparkFiles.get("dbcp.properties")))
    val source = new Properties()
    source.load(in)
    in.close()
    source.list(System.out)

    val url = source.getProperty("mysqlurl")
    val prop = new Properties()
    prop.put("user",source.getProperty("uname"))
    prop.put("password",source.getProperty("pwd"))
    ssc.checkpoint(source.getProperty("checkpointPath"))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER).map(_._2)

    //将日志生成时间加到json数据中
    val jsonStream = lines.filter(x=>x.split("\t").size==3).mapPartitions(x=>
      x.map(y=>{
        val createTime = y.split("\t")(0).substring(0, 10)
        y.split("\t")(2).replace("{","{\"createTime\":\""+createTime+"\",")
      }))

    jsonStream.foreachRDD(rdd=>
      if(!rdd.isEmpty){
        val sqlContext = SQLContextSingleton.getInstance(rdd.context)
        import sqlContext.implicits._
        //转化成DataFrame
        val content = sqlContext.read.json(rdd)
        //将DataFrame中数据取出并转化成相应的DataFrame
        val tuple = getTable(content)
        val time = tuple.map(x=>(x._3._1.replace("-",""),x._3._2))
        val current = time.max
        val before = time.min
        //查询条件，即where后边的语句 (day,hour,beforeDay,beforeHour)
        val predicates = getPredicates(before._1,current._1,before._2,current._2)
        //统计出当前时段的imp，click等
        val counts = tuple.map(x=>(x._1,x._2)).reduceByKey((x, y)=>(x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6, x._7 + y._7))
        //获取mysql中的数据，并转化成相应的dataFrame
        val mysqlData = getMysqlData(sqlContext, url, mysqlTable, prop, predicates)
        //两个表做连接查询
        val joinData = counts.leftOuterJoin(mysqlData)
        //如果左表中存在的key在右表中不存在，说明是新的数据
        val newData = getNewData(joinData.filter(_._2._2.isEmpty)).toDF("client","group","ad","creative","tag","hour","day","platform","channel","placement","imp","click","cost","download","follow","install","active")
        //如果在左右两个表中都存在相应的key，说明是要updata的数据
        val updateData = getUpdateData(joinData.filter(_._2._2.nonEmpty)).toDF("client","group","ad","creative","tag","hour","day","platform","channel","placement","imp","click","cost","download","follow","install","active","id")
        //将新数据倒入mysql
        sendNewData(newData,mysqlTable)
        //更新mysql数据库
        sendUpdateData(updateData,mysqlTable)
      })

    ssc.start()
    ssc.awaitTermination()
  }


  def sendUpdateData(updateData: DataFrame,mysqlTable:String) {
    updateData.foreachPartition(x => if(x.nonEmpty){
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = s"INSERT INTO $mysqlTable(impression,click,cost,download,follow,install,active,create_at,id)"+
                "VALUES(?,?,?,?,?,?,?,?,?)"+
                "ON DUPLICATE KEY UPDATE impression=VALUES(impression),click=VALUES(click),cost=VALUES(cost),download=VALUES(download),follow=VALUES(follow),install=VALUES(install),active=VALUES(active),create_at=VALUES(create_at)"
      try {
        conn = DataSourceFactory.getConnection
        conn.setAutoCommit(false)
        ps = conn.prepareStatement(sql)
        x.foreach(y => {
          ps.setInt(1, y.getInt(y.fieldIndex("imp")))
          ps.setInt(2, y.getInt(y.fieldIndex("click")))
          ps.setDouble(3, y.getDouble(y.fieldIndex("cost")))
          ps.setInt(4, y.getInt(y.fieldIndex("download")))
          ps.setInt(5, y.getInt(y.fieldIndex("follow")))
          ps.setInt(6, y.getInt(y.fieldIndex("install")))
          ps.setInt(7, y.getInt(y.fieldIndex("active")))
          ps.setString(8, getCurrentTime)
          ps.setInt(9,y.getInt(y.fieldIndex("id")))
          ps.addBatch()
        })
        ps.executeBatch()
        conn.commit()
      } catch {
        case e: Exception =>
          conn.rollback()
          e.printStackTrace()
      } finally {
        DataSourceFactory.closeCon(null,ps,conn)
      }
    })
  }

  def sendNewData(newData: DataFrame,mysqlTable:String) {
    newData.foreachPartition(x => if(x.nonEmpty){
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = s"insert into $mysqlTable(client_id,group_id,ad_id,impression,click,cost,download,follow,install,active,`hour`,`day`,platform,tag_id,channel_id,creative_id,placementid) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
      try {
        conn = DataSourceFactory.getConnection
        x.foreach(y => {
          ps = conn.prepareStatement(sql)
          ps.setString(1, y.getString(y.fieldIndex("client")))
          ps.setInt(2, y.getInt(y.fieldIndex("group")))
          ps.setInt(3, y.getInt(y.fieldIndex("ad")))
          ps.setInt(4, y.getInt(y.fieldIndex("imp")))
          ps.setInt(5, y.getInt(y.fieldIndex("click")))
          ps.setDouble(6, y.getDouble(y.fieldIndex("cost")))
          ps.setInt(7, y.getInt(y.fieldIndex("download")))
          ps.setInt(8, y.getInt(y.fieldIndex("follow")))
          ps.setInt(9, y.getInt(y.fieldIndex("install")))
          ps.setInt(10, y.getInt(y.fieldIndex("active")))
          ps.setInt(11, y.getInt(y.fieldIndex("hour")))
          ps.setString(12, y.getString(y.fieldIndex("day")))
          ps.setString(13, y.getString(y.fieldIndex("platform")))
          ps.setString(14, y.getString(y.fieldIndex("tag")))
          ps.setString(15, y.getString(y.fieldIndex("channel")))
          ps.setInt(16, y.getInt(y.fieldIndex("creative")))
          ps.setString(17, y.getString(y.fieldIndex("placement")))
          ps.executeUpdate()
        })
      } catch {
        case e: Exception =>
          conn.rollback()
          e.printStackTrace()
      } finally {
        DataSourceFactory.closeCon(null,ps,conn)
      }
    })
  }

  def getUpdateData(updateData: RDD[(java.lang.String, ((Int, Int, Int, Int, Int, Int, Int), Option[(Int, Int, java.math.BigDecimal, Int,Int, Int, Int, Int)]))]) = {
    updateData.mapPartitions(pars =>
      pars.map(x=>{
        val fields = x._1.split(",")
        val client = fields(0)
        val group = if (fields(1) != "") fields(1).toInt else 0
        val ad = if (fields(2) != "") fields(2).toInt else 0
        val creative = if (fields(3) != "") fields(3).toInt else 0
        val tag = fields(4)
        val hour = if (fields(5) != "") fields(5).toInt else 0
        val day = fields(6)
        val platform = fields(7)
        val channel = fields(8)
        val placement = if (fields.length == 10) fields(9) else ""
        val right = x._2._2.get
        val left = x._2._1
        val imp = left._1 + right._1
        val click = left._2 + right._2
        val cost = left._3.toDouble / 1000 + right._3.doubleValue()
        val download = left._4 + right._4
        val follow = left._5 + right._5
        val install = left._6 + right._6
        val active = left._7 + right._7
        val id = right._8
        (client, group, ad, creative, tag, hour, day, platform, channel, placement, imp, click, cost, download, follow, install, active,id)
      }))
  }

  def getNewData(newData: RDD[(java.lang.String, ((Int, Int, Int, Int, Int, Int, Int), Option[(Int, Int, java.math.BigDecimal, Int, Int, Int, Int,Int)]))]) = {
    newData.mapPartitions(pars =>
      pars.map(x=>{
        val fields = x._1.split(",")
        val client = fields(0)
        val group = if (fields(1) != "") fields(1).toInt else 0
        val ad = if (fields(2) != "") fields(2).toInt else 0
        val creative = if (fields(3) != "") fields(3).toInt else 0
        val tag = fields(4)
        val hour = if (fields(5) != "") fields(5).toInt else 0
        val day = fields(6)
        val platform = fields(7)
        val channel = fields(8)
        val placement = if (fields.length == 10) fields(9) else ""
        val imp = x._2._1._1
        val click = x._2._1._2
        val cost = x._2._1._3.toDouble/1000
        val download = x._2._1._4
        val follow = x._2._1._5
        val install = x._2._1._6
        val active = x._2._1._7
        (client, group, ad, creative, tag, hour, day, platform, channel, placement, imp, click, cost, download, follow, install, active)
      }))
  }

  def getMysqlData(sqlContext: SQLContext, url: String, mysqlTable: String, prop: Properties, predicates: Array[String]) = {
    sqlContext.read.jdbc(url, mysqlTable, predicates, prop).mapPartitions(x=>
      x.map(r => {
        val id = r.getInt(r.fieldIndex("id"))
        val client = r.getString(r.fieldIndex("client_id"))
        val group = r.getInt(r.fieldIndex("group_id"))
        val ad = r.getInt(r.fieldIndex("ad_id"))
        val imp = r.getInt(r.fieldIndex("impression"))
        val click = r.getInt(r.fieldIndex("click"))
        val cost = r.getDecimal(r.fieldIndex("cost"))
        val download = r.getInt(r.fieldIndex("download"))
        val follow = r.getInt(r.fieldIndex("follow"))
        val install = r.getInt(r.fieldIndex("install"))
        val active = r.getInt(r.fieldIndex("active"))
        val hour = r.getInt(r.fieldIndex("hour"))
        val day = r.getDate(r.fieldIndex("day"))
        val platform = if (r.getString(r.fieldIndex("platform")) == null) "" else r.getString(r.fieldIndex("platform"))
        val tag = if (r.getString(r.fieldIndex("tag_id")) == null) "" else r.getString(r.fieldIndex("tag_id"))
        val channel = if (r.getString(r.fieldIndex("channel_id")) == null) "" else r.getString(r.fieldIndex("channel_id"))
        val creative = r.getInt(r.fieldIndex("creative_id"))
        val placement = if (r.getString(r.fieldIndex("placementid")) == null) "" else r.getString(r.fieldIndex("placementid"))
        val key = client + "," + group + "," + ad + "," + creative + "," + tag + "," + hour + "," + day + "," + platform + "," + channel + "," + placement
        (key.replace(" ",""),(imp, click, cost, download, follow, install, active,id))
      }
      ))
  }

  def getPredicates(beforeDay: String, currentDay: String, beforeHour: Int, currentHour: Int): Array[String] = {
    Array("day=" + beforeDay + " and hour=" + beforeHour, "day=" + currentDay + " and hour=" + currentHour)
  }

  def getDayAndHour = {
    val format = new SimpleDateFormat("yyyyMMdd HH")
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(System.currentTimeMillis())
    val now = format.format(calendar.getTime)
    val day = now.split(" ")(0)
    val hour = now.split(" ")(1).toInt
    calendar.add(Calendar.HOUR_OF_DAY, -1)
    val before = format.format(calendar.getTime)
    val beforeDay = before.split(" ")(0)
    val beforeHour = before.split(" ")(1).toInt
    (day, hour, beforeDay, beforeHour)
  }

  def getCurrentTime={
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time = format.format(System.currentTimeMillis())
    time
  }

  def getContent(row:Row,name:String)={
    if (!row.schema.fieldNames.contains(name)||row.isNullAt(row.fieldIndex(name))) ""
    else row.getString(row.fieldIndex(name))
  }

  def getTable(content:DataFrame)= {
    content.filter(content("hqClientId").isNotNull).map(x => {
      val client = getContent(x, "hqClientId")
      val hqGroupId = getContent(x, "hqGroupId")
      val hqAdId = getContent(x, "hqAdId")
      val hqEvent = getContent(x, "hqEvent").toInt
      val hqSource = getContent(x, "hqSource")
      val platform = if (hqSource == "bc") "baidu_bc" else hqSource
      val tagId = getContent(x, "tagId")
      val channelId = getContent(x, "channelId")
      val hqCreativeId = getContent(x, "hqCreativeId")
      val hqPlacementId = getContent(x, "plmtid")//plmtid
      val hqPrice = getContent(x, "hqPrice")
      val impression = if (hqEvent == 1) 1 else 0
      val click = if (hqEvent == 2) 1 else 0
      val price = if (((hqSource == "bc" || hqSource == "tencent") && hqEvent == 1) || hqEvent == 3) hqPrice.toInt else 0
      val download = if (hqEvent == 4) 1 else 0
      val follow = if (hqEvent == 5) 1 else 0
      val install = if (hqEvent == 6) 1 else 0
      val active = if (hqEvent == 7) 1 else 0
      val creatTime = getContent(x, "createTime")
      val day = creatTime.substring(0, 4) + "-" + creatTime.substring(4, 6) + "-" + creatTime.substring(6, 8)
      val hour = creatTime.substring(8).toInt
      val key = client + "," + hqGroupId + "," + hqAdId + "," + hqCreativeId + "," + tagId + "," + hour + "," + day + "," + platform + "," + channelId + "," + hqPlacementId
      (key.replace(" ",""), (impression, click, price, download, follow, install, active),(day,hour))
    })
  }
}

object SQLContextSingleton {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}