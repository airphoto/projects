import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Calendar

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.{BrokerNotAvailableException, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{Json, ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable

/**
 * Created by hadoop on 2016/4/13.
 */
object LogStreamingDirectTest3 {
  def main(args: Array[String]) {
    val kafkaBrokers ="10.136.49.179:9092,10.136.49.179:9093"
    val zkServers = "10.136.49.179:2181"
    val topic = "test3"
    val groupid = "test-consumer-group"
    val kafkaParams = Map("bootstrap.servers" -> kafkaBrokers,
      "group.id" -> groupid
    )

    val sparkConf = new SparkConf()
      .setAppName("log streaming")

    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(30))
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val url = "jdbc:mysql://10.254.171.85:3306/haoqu_dev"
    val username="root"
    val pwd="myHq6R%![r3MJ"

    val zkClient = new ZkClient(zkServers, 2000, 2000, ZKStringSerializer)
    val partitionsAndTopics = ZkUtils.getPartitionsForTopics(zkClient, List(topic))
    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    partitionsAndTopics.foreach(topic2Partitions => {
      val topic : String = topic2Partitions._1
      val partitions : Seq[Int] = topic2Partitions._2
      val topicDirs = new ZKGroupTopicDirs(groupid, topic)

      partitions.foreach(partition => {
        val zkPath = s"${topicDirs.consumerOffsetDir}/$partition"
        ZkUtils.makeSurePersistentPathExists(zkClient, zkPath)
        val untilOffset = zkClient.readData[String](zkPath)
        val tp = TopicAndPartition(topic, partition)
        val offset = try {
          if (untilOffset == null || untilOffset.trim == "")
            getMaxOffset(tp, zkClient)
          else
            untilOffset.toLong
        } catch {
          case e: Exception => getMaxOffset(tp, zkClient)
        }
        fromOffsets += (tp -> offset)
        println(s"Offset init: set offset of $topic/$partition as $offset")
      })
    })

    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)

    var offsetRanges = Array[OffsetRange]()
    messages.foreachRDD(rdd => {
      val json = rdd.map(_._2).filter(x => x.split("\t").size == 3).map(x => {
        val createTime = x.split("\t")(0)
        x.split("\t")(2).replace("{", "{\"createTime\":\"" + createTime + "\",")
      })

      val content = sqlContext.read.json(json)
      val table = content.map(x => {
        val client = getContent(x, "hqClientId")
        val group = getContent(x, "hqGroupId")
        val ad = getContent(x, "hqAdId")
        val event = getContent(x, "hqEvent")
        val source = getContent(x, "hqSource")
        val platform = if(source=="bc") "baidu_bc" else source
        val tag = getContent(x, "tagId")
        val channel = getContent(x, "channelId")
        val creative = getContent(x, "hqCreativeId")
        val placement = getContent(x, "hqPlacementId")
        val price = if(getContent(x, "hqPrice").isEmpty) "0".toDouble else getContent(x, "hqPrice").toDouble
        val time = getContent(x, "createTime")
        val imp = if(event=="1") 1 else 0
        val click = if(event=="2") 1 else 0
        val download = if(event=="4") 1 else 0
        val follow = if(event=="5") 1 else 0
        val install = if(event=="6") 1 else 0
        val active = if(event=="7") 1 else 0
        val cost = if(((platform=="baidu_bc"||platform=="tencent")&&event==1)||((platform!="baidu_bc"||platform!="tencent")&&event==3)) price else 0.0
        val day = time.substring(0, 8).toInt
        val hour = time.substring(8, 10).toInt
        (client, group, ad, platform, tag, channel, creative, placement, day, hour,cost,imp,click,download,follow,install,active)
      }).toDF("hqclientid", "hqgroupid", "hqadid", "hqsource", "tagid", "channelid", "hqcreativeid", "hqplacementid", "day", "hour","cost","imp","click","download","follow","install","active")

      val max = table.agg("hour"->"max","day"->"max").first
      val min = table.agg("hour"->"min","day"->"min").first
      val maxHour = max.getInt(0)
      val minHour = min.getInt(0)
      val minDay = min.getInt(1)
      val maxDay = max.getInt(1)

      val data = table.groupBy("hqclientid", "hqgroupid", "hqadid", "hqsource", "tagid", "channelid", "hqcreativeid", "hqplacementid","day", "hour").sum("cost","imp","click","download","follow","install","active")
      .toDF("hqclientid", "hqgroupid", "hqadid", "platform", "tagid", "channelid", "hqcreativeid", "hqplacementid","day", "hour","cost","impression","click","download","follow","install","active")
      .map(r => {
        val client = r.getString(r.fieldIndex("hqclientid"))
        val group = if (r.getString(r.fieldIndex("hqgroupid")) == "") "0" else r.getString(r.fieldIndex("hqgroupid"))
        val ad = if (r.getString(r.fieldIndex("hqadid")) == "") "0" else r.getString(r.fieldIndex("hqadid"))
        val imp = r.getLong(r.fieldIndex("impression"))
        val click = r.getLong(r.fieldIndex("click"))
        val cost = r.getDouble(r.fieldIndex("cost"))/1000
        val download = r.getLong(r.fieldIndex("download"))
        val follow = r.getLong(r.fieldIndex("follow"))
        val install = r.getLong(r.fieldIndex("install"))
        val active = r.getLong(r.fieldIndex("active"))
        val hour = r.getInt(r.fieldIndex("hour"))
        val day = r.getInt(r.fieldIndex("day")).toString
        val mysqlDay = day.substring(0, 4) + "-" + day.substring(4, 6) + "-" + day.substring(6)
        val platform = r.getString(r.fieldIndex("platform"))
        val tag = r.getString(r.fieldIndex("tagid"))
        val channel = r.getString(r.fieldIndex("channelid"))
        val creative = r.getString(r.fieldIndex("hqcreativeid"))
        val placement = r.getString(r.fieldIndex("hqplacementid"))
        val key = client + "," + group + "," + ad + "," + creative + "," + tag + "," + hour + "," + mysqlDay + "," + platform + "," + channel + "," + placement
        (key.trim, imp, click, cost, download, follow, install, active)
      }).toDF("keys", "imps", "clicks", "costs", "loads", "follows", "installs", "actives")

      data.show(10,false)

      val mysqlData = rdd.sparkContext.parallelize {
        var conn: Connection = null
        var ps: PreparedStatement = null
        var r: ResultSet = null
        val arr = scala.collection.mutable.ArrayBuffer[Tuple8[String, Int, Int, BigDecimal, Int, Int, Int, Int]]()
        try {
          conn = DriverManager.getConnection(url, username, pwd)
          if(maxDay==minDay){
            ps = conn.prepareStatement("select * from hq_all_report where day=? and hour>=? and hour<=?")
            ps.setInt(1, maxDay)
            ps.setInt(2, minHour)
            ps.setInt(3,maxHour)
          }else{
            ps = conn.prepareStatement("select * from hq_all_report where (day=? and hour=?) or (day=? and hour=?)")
            ps.setInt(1, maxDay)
            ps.setInt(2, minHour)
            ps.setInt(3, minDay)
            ps.setInt(4,maxHour)
          }

          r = ps.executeQuery()
          while (r.next()) {
            val client = r.getString("client_id")
            val group = r.getInt("group_id")
            val ad = r.getInt("ad_id")
            val imp = r.getInt("impression")
            val click = r.getInt("click")
            val cost = r.getBigDecimal("cost")
            val download = r.getInt("download")
            val follow = r.getInt("follow")
            val install = r.getInt("install")
            val active = r.getInt("active")
            val hour = r.getInt("hour")
            val day = r.getDate("day")
            val platform = if (r.getString("platform") == null) "" else r.getString("platform")
            val tag = if (r.getString("tag_id") == null) "" else r.getString("tag_id")
            val channel = if (r.getString("channel_id") == null) "" else r.getString("channel_id")
            val creative = r.getInt("creative_id")
            val placement = if (r.getString("placementid") == null) "" else r.getString("placementid")
            val key = client + "," + group + "," + ad + "," + creative + "," + tag + "," + hour + "," + day + "," + platform + "," + channel + "," + placement
            arr += Tuple8(key.trim, imp, click, cost, download, follow, install, active)
          }
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          if (r != null) {
            r.close()
            r = null
          }
          if (ps != null) {
            ps.close()
            ps = null
          }
          if (conn != null) {
            conn.close()
            conn = null
          }
        }
        arr
      }.toDF("key","imp","click","cost","load","follow","install","active")

      mysqlData.show(10,false)

      //两个表做连接查询
      val joinData = data.join(mysqlData,$"keys"===$"key","left_outer")

      val newData = joinData.filter(joinData("key").isNull).map(x => {
        val fields = x.getString(x.fieldIndex("keys")).split(",")
        val client = fields(0)
        val group = if(fields(1)!="") fields(1).toInt else 0
        val ad = if(fields(2)!="") fields(2).toInt else 0
        val creative = if(fields(3)!="") fields(3).toInt else 0
        val tag = fields(4)
        val hour = if(fields(5)!="") fields(5).toInt else 0
        val day = fields(6)
        val platform = fields(7)
        val channel = fields(8)
        val placement = if (fields.length==10) fields(9) else ""
        val imp = x.getLong(x.fieldIndex("imps")).toInt
        val click = x.getLong(x.fieldIndex("clicks")).toInt
        val cost = x.getDouble(x.fieldIndex("costs"))
        val download = x.getLong(x.fieldIndex("loads")).toInt
        val follow = x.getLong(x.fieldIndex("follows")).toInt
        val install = x.getLong(x.fieldIndex("installs")).toInt
        val active = x.getLong(x.fieldIndex("actives")).toInt
        (client, group, ad, creative, tag, hour, day, platform, channel, placement, imp, click, cost, download, follow, install, active)
      }).toDF("client","group","ad","creative","tag","hour","day","platform","channel","placement","imp","click","cost","download","follow","install","active")

      val updateData = joinData.filter(joinData("key").isNotNull).map(x => {
        val fields = x.getString(x.fieldIndex("keys")).split(",")
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
        val imp = x.getLong(x.fieldIndex("imps")) + x.getInt(x.fieldIndex("imp"))
        val click = x.getLong(x.fieldIndex("clicks")) + x.getInt(x.fieldIndex("click"))
        val cost = x.getDouble(x.fieldIndex("costs")) + x.get(x.fieldIndex("cost")).toString.toDouble
        val download = x.getLong(x.fieldIndex("loads")) + x.getInt(x.fieldIndex("load"))
        val follow = x.getLong(x.fieldIndex("follows")) + x.getInt(x.fieldIndex("follow"))
        val install = x.getLong(x.fieldIndex("installs")) + x.getInt(x.fieldIndex("install"))
        val active = x.getLong(x.fieldIndex("actives")) + x.getInt(x.fieldIndex("active"))
        (client, group, ad, creative, tag, hour, day, platform, channel, placement, imp.toInt, click.toInt, cost, download.toInt, follow.toInt, install.toInt, active.toInt)
      }).toDF("client","group","ad","creative","tag","hour","day","platform","channel","placement","imp","click","cost","download","follow","install","active")

      newData.foreachPartition(x => {
        var conn: Connection = null
        var ps: PreparedStatement = null
        val sql = "insert into hq_all_report(client_id,group_id,ad_id,impression,click,cost,download,follow,install,active,`hour`,`day`,platform,tag_id,channel_id,creative_id,placementid) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        try {
          conn = DriverManager.getConnection(url, username, pwd)
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
          case e: Exception => e.printStackTrace()
        } finally {
          if (ps != null) {
            ps.close()
            ps = null
          }
          if (conn != null) {
            conn.close()
            conn = null
          }
        }
      })
      updateData.foreachPartition(x => {
        var conn: Connection = null
        var ps: PreparedStatement = null
        val sql = "update hq_all_report set impression=?,click=?,cost=?,download=?,follow=?,install=?,active=?,create_at=? where " +
          "`hour`=? and `day`=? and platform=? and tag_id=? and channel_id=? and creative_id=? and client_id=? and group_id=? and ad_id=? and placementid=?"
        try {
          conn = DriverManager.getConnection(url, username, pwd)
          x.foreach(y => {
            ps = conn.prepareStatement(sql)
            ps.setInt(1, y.getInt(y.fieldIndex("imp")))
            ps.setInt(2, y.getInt(y.fieldIndex("click")))
            ps.setDouble(3, y.getDouble(y.fieldIndex("cost")))
            ps.setInt(4, y.getInt(y.fieldIndex("download")))
            ps.setInt(5, y.getInt(y.fieldIndex("follow")))
            ps.setInt(6, y.getInt(y.fieldIndex("install")))
            ps.setInt(7, y.getInt(y.fieldIndex("active")))
            ps.setString(8, getCurrentTime)
            ps.setInt(9, y.getInt(y.fieldIndex("hour")))
            ps.setString(10, y.getString(y.fieldIndex("day")))
            ps.setString(11, y.getString(y.fieldIndex("platform")))
            ps.setString(12, y.getString(y.fieldIndex("tag")))
            ps.setString(13, y.getString(y.fieldIndex("channel")))
            ps.setInt(14, y.getInt(y.fieldIndex("creative")))
            ps.setString(15, y.getString(y.fieldIndex("client")))
            ps.setInt(16, y.getInt(y.fieldIndex("group")))
            ps.setInt(17, y.getInt(y.fieldIndex("ad")))
            ps.setString(18, y.getString(y.fieldIndex("placement")))
            ps.executeUpdate()
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          if (ps != null) {
            ps.close()
            ps = null
          }
          if (conn != null) {
            conn.close()
            conn = null
          }
        }
      })

    })

    messages.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD(rdd => {
      offsetRanges.foreach(o => {
        val topicDirs = new ZKGroupTopicDirs(groupid, o.topic)
        val zkOffsetPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        ZkUtils.updatePersistentPath(zkClient, zkOffsetPath, o.untilOffset.toString)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def getDayAndHour={
    val format = new SimpleDateFormat("yyyyMMdd HH")
    val time = format.format(System.currentTimeMillis())
    val day = time.split(" ")(0).toInt
    val hour = time.split(" ")(1).toInt
    (day,hour)
  }

  def getCurrentTime={
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val cal = Calendar.getInstance()
    val time = format.format(cal.getTime)
    time
  }

  def getContent(row:Row,name:String)={
    if (!row.schema.fieldNames.contains(name)) ""
    else row.getString(row.fieldIndex(name))
  }



  private def getMaxOffset(tp : TopicAndPartition, zkClient: ZkClient):Long = {

    val request = OffsetRequest(immutable.Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))

    ZkUtils.getLeaderForPartition(zkClient, tp.topic, tp.partition) match {
      case Some(brokerId) => {
        ZkUtils.readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath + "/" + brokerId)._1 match {
          case Some(brokerInfoString) => {
            Json.parseFull(brokerInfoString) match {
              case Some(m) =>
                val brokerInfo = m.asInstanceOf[Map[String, Any]]
                val host = brokerInfo.get("host").get.asInstanceOf[String]
                val port = brokerInfo.get("port").get.asInstanceOf[Int]
                new SimpleConsumer(host, port, 10000, 100000, "getMaxOffset")
                  .getOffsetsBefore(request)
                  .partitionErrorAndOffsets(tp)
                  .offsets
                  .head
              case None =>
                throw new BrokerNotAvailableException("Broker id %d does not exist".format(brokerId))
            }
          }
          case None =>
            throw new BrokerNotAvailableException("Broker id %d does not exist".format(brokerId))
        }
      }
      case None =>
        throw new Exception("No broker for partition %s - %s".format(tp.topic, tp.partition))
    }
  }

}


