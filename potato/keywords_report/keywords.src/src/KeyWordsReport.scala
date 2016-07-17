import java.io.InputStream
import java.text.SimpleDateFormat
import java.util.{Calendar, UUID, Properties, Date}

import com.rabbitmq.client.{MessageProperties, Channel, Connection, ConnectionFactory}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by hadoop on 2016/5/11.
 */
object KeyWordsReport {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("keywordsReport")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val stream = KeyWordsReport.getClass.getClassLoader.getResourceAsStream("placement.properties")
    val property = getProperties(stream){prop=>prop.load(stream)}

    import sqlContext.implicits._

    sqlContext.sql("use formatlog")

    //获取接收的rabbitmq中的json数据
    val filterCondition = sc.parallelize(List(args(0)))
    //转化成DataFrame
    val filterJson = sqlContext.read.json(filterCondition)
    val row = filterJson.first()

    //获取platform
    val platform = getPlatform(row)
    //获取keywords的路径
    val keywordsPath = row.getString(row.fieldIndex("path")).replace(":","/")
    //id原样放回
    val id = row.getLong(row.fieldIndex("id"))

    //获取keywords，并将其转化成HashMap，其中值需要keySet
    val keywordSet = sc.textFile(property.getProperty("keywordsPre")+"/"+keywordsPath).map(x=>(x,None)).collectAsMap()

    //获取当天的数据
    val bidData =  sqlContext.table(getTable(platform)).where("d="+getDay)

    //过滤出需要的数据
    val filterData = getFilterData(property,row,bidData).filter(x=> {
      val keywords = x._6
      if(keywords.size>0) {
        val tmp = for (i <- 0 until keywords.size) yield keywordSet.keySet.contains(keywords.get(i)) //keySet中是否包含该行数据的keyword
        tmp.toSet[Boolean].contains(true)
      }else false
    }
    )

    //根据channel，hour 统计数据
    val count = filterData.map(x=>{
      (x._2,x._7,1)
    }).toDF("channel","hour","flag").groupBy("channel","hour").sum("flag").toDF("channel","hour","counts")

    val result = count.map(x=>{
      val channel = x.getString(x.fieldIndex("channel"))
      val hour = x.getString(x.fieldIndex("hour")).toInt
      val count = x.get(x.fieldIndex("counts")).toString.toInt
      (platform,channel,hour,count)
    }).filter(x=>(x._2!=null && !x._2.isEmpty)).toDF("platform","channel","hour","count").toJSON

    //转化成json数据并发送到rabbitmq中
    sc.parallelize(List(result.collect().mkString(","))).map(x=>{
      "{\"id\":"+id+",\"data\":["+x+"]}"
    }).coalesce(1).foreach(x=>{
      SendMessage(property, x)
    })
    sc.stop()
  }

  def getPlatform(row: Row) = {
    if (row.schema.fieldNames.contains("platform")) row.getString(row.fieldIndex("platform")) else "youku"
  }

  /**
   * 向rabbitmq中传送数据
   * @param property
   * @param x
   */
  def SendMessage(property: Properties, x: String) {
    val QUEUE_NAME = property.getProperty("QUEUE_NAME")
    val factory: ConnectionFactory = new ConnectionFactory
    factory.setHost(property.getProperty("host"))
    factory.setUsername(property.getProperty("username"))
    factory.setPassword(property.getProperty("password"))
    val connection: Connection = factory.newConnection
    val channel: Channel = connection.createChannel

    channel.queueDeclare(QUEUE_NAME, true, false, false, null)
    channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, x.getBytes("UTF-8"))

    channel.close
    connection.close
  }

  //获取前一天的日期
  def getDay={
    val format = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance()
    calendar.setTime(new Date())
    calendar.add(Calendar.DAY_OF_MONTH,-1)
    format.format(calendar.getTime)
  }

  //platform 和表的对应关系
  def getTable(platform:String) = {
    val table = platform match {
      case "youku" => "bid_request"
    }
    table
  }

  def getFilterData(property:Properties,row:Row,bidData:DataFrame)={

    val placement = getFilterColumn(row,"placementid")
    val channel = getFilterColumn(row,"channel")
    val size = getFilterColumn(row,"size")
    val device = getFilterColumn(row,"device")
    val systems = getFilterColumn(row,"system")

    val data = transform(property,bidData).filter(x=>{
      (channel.isEmpty || channel.contains(x._2)) && (device.isEmpty || device.contains(x._3)) && (size.isEmpty || size.contains(x._4)) && (systems.isEmpty || systems.contains(x._5)) && (placement.isEmpty || placement.contains(x._8))
    })

    data
  }

  def getFilterColumn(row:Row,name:String)= {
    if(row.schema.fieldNames.contains(name)) row.getList[String](row.fieldIndex(name)) else java.util.Collections.emptyList[String]()
  }

  /**
   * 获取表中需要的列，并根据需要转化
   * @param property
   * @param bidData
   * @return
   */
  def transform(property:Properties,bidData: DataFrame) = {
    bidData.map(x => {
      val hour = x.getString(x.fieldIndex("h"))
//
      val chanCol = x.getString(x.fieldIndex("channel"))
//
      val tagid = x.getString(x.fieldIndex("tagid"))
//
      val placementid = getPlacemendId(property,tagid,chanCol)
//
      val devCol = x.getString(x.fieldIndex("site_or_app_name"))
      val device = if(devCol!=null && !devCol.isEmpty) (if(devCol.contains("客户端")) "Mobile" else "PC") else "other"
      val height = x.getInt(x.fieldIndex("height"))
      val width = x.getInt(x.fieldIndex("width"))
      val sizeCol = width + "x" + height
      val sysCol = x.getString(x.fieldIndex("os"))
      val system = if(sysCol!=null && !sysCol.isEmpty) getSystem(sysCol) else "Other"
      val keyCol = if(!x.isNullAt(x.fieldIndex("keywords"))) x.getList[String](x.fieldIndex("keywords")) else java.util.Collections.emptyList[String]()
      val keywords =  keyCol
      ("", chanCol, device, sizeCol, system, keywords,hour,placementid)
    })
  }

  def getPlacemendId(properties:Properties,tagid:String,channel:String)={
    val a = properties.getProperty("a").split(",")
    val k = properties.getProperty("k").split(",")
    val z = properties.getProperty("z").split(",")
    val tv=properties.getProperty("tv").split(",")


    val placementPre = if (a.contains(tagid)) "a" else if (k.contains(tagid)) "k" else if (z.contains(tagid)) "z" else tagid
    val placementTail = if (tv.contains(channel)) "tv" else "ugc"

    //placement是根据tagid和channel做的拼接
    if (placementPre == tagid) tagid else placementPre + placementTail
  }

  def getSystem(ua:String)={
    if(ua.contains("windows")) "Windows"
    else if(ua.contains("linux")) "Linux"
    else if(ua.contains("mac")) "Mac"
    else if(ua.contains("iphone") || ua.contains("ipad") || ua.contains("ios")) "IOS"
    else if(ua.contains("android")) "Android"
    else "Other"
  }

  /**
   * 获取配置文件
   * @param inputStream
   * @return
   */
  def getProperties(inputStream: InputStream)(op:Properties=>Unit)={
    val properties = new Properties()
    try{
      op(properties)
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      inputStream.close()
    }
    properties
  }
}
