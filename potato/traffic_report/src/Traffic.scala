//import java.text.SimpleDateFormat

import java.io.InputStream

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import com.rabbitmq.client.{Channel, Connection, ConnectionFactory, MessageProperties}
//import java.util.Calendar
import java.util.Properties

//import sys.process._中文

object Traffic {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    val sqlContext = new HiveContext(sc)

    val jsondataV = args(0)
    println(jsondataV)
    val platformCN = Map("tencent" -> "腾讯视频", "iqiyi" -> "爱奇艺", "letv" -> "乐视", "youku" -> "优酷视频", "baidu_bc" -> "百度百川")

    var (platformV, fieldsV, beginDay, endDay, deviceV, channelV, sizeV, osV, tagidV, placementidV, idV) = ("", "", "", "", "", "", "", "", "", "", "")
    val result_input = scala.util.parsing.json.JSON.parseFull(jsondataV)
    result_input match {
      case Some(e:Map[String,String]) => {
        println(e);
        e.foreach { pair =>
          println(pair._1 + ":" + pair._2)
        }
        platformV = e("platform")
        fieldsV = e("columns")
        beginDay = e("beginday")
        endDay = e("endday")
        deviceV = e("device")
        channelV = e("channel")
        sizeV = e("size")
        osV = e("os")
        tagidV = e("tagid")
        placementidV = e("placementid")
        idV = e("id")
      }
      case None => println("Failed.")
    }

    var query_table_name = ""
    println(platformV)
    platformV match {
      case "youku" =>
        query_table_name = "bid_request"
      case "iqiyi" =>
        query_table_name = "bidder_iqiyi"
    }

    sqlContext.sql("use formatlog")
    sqlContext.sql("drop temporary function checkos")
    sqlContext.sql("create temporary function checkos as 'com.hq.traffic.udf.Os'")

    val fieldsnames = getDynamicTableFields(fieldsV)
    val tmp_table_str =s"""create table IF NOT EXISTS hq_traffic_src(
       $fieldsnames
       )
       PARTITIONED BY(d string)
       ROW FORMAT DELIMITED
       FIELDS TERMINATED BY '\t'"""

    // sqlContext.sql("DROP TABLE  IF EXISTS hq_traffic_src")

    // println(s"$tmp_table_str")

    // sqlContext.sql(s"$tmp_table_str")
    // sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val sql_select_str = s"SELECT '$platformV' AS platform, count(*) AS count"
    val sql_where_str = s"where d>=$beginDay and d<=$endDay"
    val sql_group_str = s"group by '$platformV'"

    val (dynasql_select_str, dynasql_where_str, dynasql_group_str) = getDynamicSql(platformV, deviceV, channelV, sizeV, osV, tagidV, placementidV, fieldsV, sql_select_str, sql_where_str, sql_group_str)

    val query_table_str =s"""INSERT OVERWRITE TABLE hq_traffic_src PARTITION(d)
     $dynasql_select_str
     FROM bid_request
     $dynasql_where_str
     $dynasql_group_str"""

    // sqlContext.sql(s"$query_table_str")
    val query_table_str2 =s"""$dynasql_select_str
     FROM $query_table_name
     $dynasql_where_str
     $dynasql_group_str"""

    println(s"$query_table_str2")
    val df = sqlContext.sql(s"$query_table_str2")
    val fields_arr = fieldsV.split(",").distinct

    //val format = new SimpleDateFormat("yM")
    println("after execute query!!!")
    //val now_ym = format.format(Calendar.getInstance().getTime())
    val r = scala.util.Random

    val outPath = "/user/work/reports/media_traffic/"+idV+"_"+r.nextInt(1000000).toString
    println(outPath)
    //"hadoop fs -rm -r /user/work/reports/media_traffic/".!


      df.map(x=>{
        var (ret_commadeli_str, field_name_val, field_val) = ("", "", "")
        for (field_name <- fields_arr) {
          field_name_val = field_name.trim
          field_val = x.getString(x.fieldIndex(field_name_val))
          if (field_name_val == "day") {
            field_val = field_val.substring(0, 4) + "-" + field_val.substring(4, 6) + "-" + field_val.substring(6)
          }
          if (field_name_val == "platform") {
            field_val = platformCN(field_val)
          }
          ret_commadeli_str = ret_commadeli_str + field_val + ","
        }
        //val platf = x.getString(x.fieldIndex("platform"))
        val counts = x.getLong(x.fieldIndex("count"))
        ret_commadeli_str = ret_commadeli_str + counts
        ret_commadeli_str
      }).coalesce(1).saveAsTextFile(outPath)
    //df.write.format("com.databricks.spark.csv").save("/data/home.csv")
    //转化成json数据并发送到rabbitmq中
    try {
      println("{\"id\":"+idV+",\"status\":1,\"path\":\""+outPath+"/part-00000\"}")
      SendMessage("{\"id\":"+idV+",\"status\":1,\"path\":\""+outPath+"/part-00000\"}")
      println("Send Message Back Successfully!")
    } catch {
      case e: Exception =>
        e.printStackTrace
        SendMessage("{\"id\":"+idV+",\"status\":0}")
    }

    sc.stop()
  }

  def SendMessage(sidfilename: String) {
    val stream = Traffic.getClass.getClassLoader.getResourceAsStream("rabbitmq_server_config.properties")
    val property = getProperties(stream){prop=>prop.load(stream)}
    val QUEUE_NAME = "set_webhdfs_download_report"
    val factory: ConnectionFactory = new ConnectionFactory
    val mq_host_name = property.getProperty("hostname")
    val mq_user_name = property.getProperty("username")
    val mq_password = property.getProperty("pwd")
    factory.setHost(mq_host_name)
    factory.setUsername(mq_user_name)
    factory.setPassword(mq_password)
    val connection: Connection = factory.newConnection
    val channel: Channel = connection.createChannel

    channel.queueDeclare(QUEUE_NAME, true, false, false, null)
    channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, sidfilename.getBytes("UTF-8"))

    channel.close
    connection.close
  }

  def isWithContent(x: String) = x != null && x.trim.nonEmpty

  def getDynamicTableFields(fields: String) = {
    var tb_fields ="day string, hour string, platform string, count int"

    if (isWithContent(fields)) {

      val fields_arr = fields.split(",").distinct

      for (field_name <- fields_arr) {
        val field_name_val = field_name.trim

        field_name_val match {
          case "device" =>
            tb_fields = s"$tb_fields, $field_name_val string"
          case "channel" =>
            tb_fields = s"$tb_fields, $field_name_val string"
          case "size" =>
            tb_fields = s"$tb_fields, $field_name_val string"
          case "os" =>
            tb_fields = s"$tb_fields, $field_name_val string"
          case "tagid" =>
            tb_fields = s"$tb_fields, $field_name_val string"
          case everythingElse =>
            tb_fields = s"$tb_fields, $field_name_val string"
        }

      }
    }
    tb_fields
  }

  def getDynamicSql(splatform: String, sdevice: String, schannel: String, ssize: String, sos: String, str_tagid: String, str_placementid: String, fields: String, select_sql: String, where_sql: String, group_sql: String) = {

    val (sdeviceV, schannelV, ssizeV, sosV, str_tagidV, placementidV) = (sdevice, schannel, ssize, sos, str_tagid, str_placementid)

    var (select_sql_str, where_sql_str, group_sql_str) = (select_sql, where_sql, group_sql)
    if (isWithContent(fields)) {
      val fields_arr = fields.split(",").distinct
      for (field_name <- fields_arr) {
        val field_name_val = field_name.trim
        field_name_val match {
          case "device" =>
            if (splatform == "youku") {
              select_sql_str = s"$select_sql_str, if(device_type=0,'Mobile',(if(device_type=1,'Tablet','PC'))) AS device"
              group_sql_str = s"$group_sql_str, if(device_type=0,'Mobile',(if(device_type=1,'Tablet','PC')))"
            } else {
              select_sql_str = s"$select_sql_str"
              group_sql_str = s"$group_sql_str"
            }
          case "channel" =>
            if (splatform == "youku") {
              select_sql_str = s"$select_sql_str, channel"
              group_sql_str = s"$group_sql_str, channel"
            } else {
              select_sql_str = s"$select_sql_str"
              group_sql_str = s"$group_sql_str"
            }
          case "day" =>
            select_sql_str = s"$select_sql_str, d AS day"
            group_sql_str = s"$group_sql_str, d"
          case "hour" =>
            select_sql_str = s"$select_sql_str, h AS hour"
            group_sql_str = s"$group_sql_str, h"
          case "size" =>
            if (splatform == "youku") {
              select_sql_str = s"$select_sql_str, concat(cast(width as string),'x',cast(height as string)) AS size"
              group_sql_str = s"$group_sql_str, concat(cast(width as string),'x',cast(height as string))"
            } else {
              select_sql_str = s"$select_sql_str"
              group_sql_str = s"$group_sql_str"
            }
          case "os" =>
            if (splatform == "youku") {
              select_sql_str = s"$select_sql_str, checkos(os) AS os"
              group_sql_str = s"$group_sql_str, checkos(os)"
            } else {
              select_sql_str = s"$select_sql_str"
              group_sql_str = s"$group_sql_str"
            }
          case "tagid" =>
            if (splatform == "youku") {
              select_sql_str = s"$select_sql_str, tagid"
              group_sql_str = s"$group_sql_str, tagid"
            } else {
              select_sql_str = s"$select_sql_str"
              group_sql_str = s"$group_sql_str"
            }
          case "placementid" =>
            if (splatform == "youku") {
              select_sql_str = s"$select_sql_str, placementid"
              group_sql_str = s"$group_sql_str, placementid"
            } else {
              select_sql_str = s"$select_sql_str"
              group_sql_str = s"$group_sql_str"
            }
          case everythingElse =>
            select_sql_str = s"$select_sql_str"
            group_sql_str = s"$group_sql_str"
        }

      }

    }

    if (isWithContent(sdeviceV)) {
      if (splatform == "youku") {
        where_sql_str = s"$where_sql_str and device_type in ($sdeviceV)"
      } else {
        where_sql_str = s"$where_sql_str"
      }
    }

    if (isWithContent(schannelV)) {
      if (splatform == "youku") {
        where_sql_str = s"$where_sql_str and channel in ($schannelV)"
      } else {
        where_sql_str = s"$where_sql_str"
      }
    }

    if (isWithContent(ssizeV)) {
      if (splatform == "youku") {
        where_sql_str = s"$where_sql_str and concat(cast(width as string),'x',cast(height as string)) in ($ssizeV)"
      } else {
        where_sql_str = s"$where_sql_str"
      }
    }

    if (isWithContent(sosV)) {
      if (splatform == "youku") {
        where_sql_str = s"$where_sql_str and checkos(os) in ($sosV)"
      } else {
        where_sql_str = s"$where_sql_str"
      }
    }

    if (isWithContent(str_tagidV)) {
      if (splatform == "youku") {
        where_sql_str = s"$where_sql_str and tagid in ($str_tagidV)"
      } else {
        where_sql_str = s"$where_sql_str"
      }
    }

    if (isWithContent(placementidV)) {
      if (splatform == "youku") {
        where_sql_str = s"$where_sql_str and placementid in ($placementidV)"
      } else {
        where_sql_str = s"$where_sql_str"
      }
    }

    (select_sql_str, where_sql_str, group_sql_str)
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