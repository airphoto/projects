import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by hadoop on 2016/3/2.
 */
object Anomaly{
  case class Info(idfa: String, ip: String, hour: String, day: String)

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    val begain = args(0)
    val end = args(1)
    val outPathPre = args(2)
    val part = args(3).toInt
    val database = args(4)


    sqlContext.sql("use formatlog")
    val df = sqlContext.sql("select user_id,ip,time_i from bid_request where height=480 and width=640 and d>="+begain+" and d<="+end)

    val userInfo = df.map(x=>{
      val idfa = x.getString(x.fieldIndex("user_id"))
      val ip = x.getString(x.fieldIndex("ip"))
      val ts = AnomalyUtil.DateFormat(x.getInt(x.fieldIndex("time_i")))
      val day = ts.split(" ")(0)
      val hour = ts.split(" ")(1)
      Info(idfa,ip,hour,day)
    }).toDF()

    val appleUsers = userInfo.filter(userInfo("idfa").isNotNull&&userInfo("idfa").contains("-"))//过滤非IOS用户

    //每个idfa的观看的视频总数，使用的IP的总数，一天中使用的IP最多的数量，一天最多观看视频数，观看视频的天数，观看视频总量/天数

    //视频总量/ip数，观看视频的时间段（全天分为12个时段，有则标记为1，无则标记为0）

    //用户观看的video的数量
    val vCountPerUser = appleUsers.map(x=>(x.getString(x.fieldIndex("idfa")),1)).reduceByKey(_+_).persist(StorageLevel.MEMORY_AND_DISK)

    //每个用户在统计时间内一共使用的ip数量
    val ipCountPerUser = appleUsers.select(appleUsers("idfa"),appleUsers("ip")).distinct.map(x=>(x.getString(x.fieldIndex("idfa")),1)).reduceByKey(_+_).persist(StorageLevel.MEMORY_AND_DISK)


    //统计一天中最多使用的ip数量
    val ipCountMax = appleUsers.select(appleUsers("idfa"),appleUsers("ip"),appleUsers("day")).distinct.map(x=>{
      val userid = x.getString(x.fieldIndex("idfa"))
      val d = x.getString(x.fieldIndex("day"))
      (userid+"\t"+d,1)
    }).reduceByKey(_+_).map(x=>{
      val userid = x._1.split("\t")(0)
      val count = x._2
      (userid,count)
    }).reduceByKey((x,y)=>{
      Math.max(x,y)
    }).persist(StorageLevel.MEMORY_AND_DISK)

    //每条记录以userid和day作为key
    val linePerDay = appleUsers.map(x=>{
      val userid = x.getString(x.fieldIndex("idfa"))
      val day = x.getString(x.fieldIndex("day"))
      (userid+"\t"+day,1)
    })

    //计算出每个用户每天观看的数量，并求提取最大值
    val vCountMaxPerIdfa=linePerDay.reduceByKey(_+_).map(x=>{
      val userid = x._1.split("\t")(0)
      val count = x._2
      (userid,count)
    }).reduceByKey((x,y)=>{
      Math.max(x,y)
    }).persist(StorageLevel.MEMORY_AND_DISK)

    //观看视频的天数
    val vDays = linePerDay.distinct.map(x=>{
      val userid = x._1.split("\t")(0)
      (userid,1)
    }).reduceByKey(_+_).persist(StorageLevel.MEMORY_AND_DISK)

    //观看的时间段（全天分为24个时段，有一个小时算一个小时）

    val vHours = appleUsers.map(x=>{
      val userid = x.getString(x.fieldIndex("idfa"))
      val hour = x.getString(x.fieldIndex("hour"))
      userid+"\t"+hour
    }).distinct.map(x=>{
      (x.split("\t")(0),1)
    }).reduceByKey(_+_).persist(StorageLevel.MEMORY_AND_DISK)


    val vCountPerDayIdfa = vCountPerUser.join(vDays).map(x=>{
      val userid = x._1
      val mean = x._2._1.toDouble/x._2._2.toDouble
      (userid,mean.formatted("%.2f"))
    })

    val vCountPerIpIdfa = vCountPerUser.join(ipCountPerUser).map(x=>{
      val userid = x._1
      val mean = x._2._1.toDouble/x._2._2.toDouble
      (userid,mean.formatted("%.2f"))
    })

    val ipPerDayIdfa = ipCountPerUser.join(vDays).map(x=>{
      val userid = x._1
      val mean = x._2._1.toDouble/x._2._2.toDouble
      (userid,mean.formatted("%.2f"))
    })

    //每个idfa的观看的视频总数，使用的IP的总数，一天中使用的IP最多的数量，一天最多观看视频数，观看视频的天数，观看视频总量/天数
    //视频总量/ip数，观看视频的时间段（全天分为24个时段，有一个小时算一个小时）
    val info = ipCountPerUser.join(vCountPerUser).map(x=>{
      val userid = x._1
      val tmp = x._2._1+"\t"+x._2._2//使用的IP的总数,观看的视频总数
      (userid,tmp)
    }).join(ipCountMax).map(x=>{
      (x._1,x._2._1+"\t"+x._2._2)//一天中使用的IP最多的数量
    }).join(vCountMaxPerIdfa).map(x=>{
      (x._1,x._2._1+"\t"+x._2._2)//一天最多观看视频数
    }).join(vDays).map(x=>{
      (x._1,x._2._1+"\t"+x._2._2)//观看视频的天数
    }).join(vCountPerDayIdfa).map(x=>{
      (x._1,x._2._1+"\t"+x._2._2)//观看视频总量/天数
    }).join(vCountPerIpIdfa).map(x=>{
      (x._1,x._2._1+"\t"+x._2._2)//视频总量/ip数
    }).join(vHours).map(x=>{
      (x._1,x._2._1+"\t"+x._2._2)//观看视频的时间段
    }).join(ipPerDayIdfa).map(x=>{
      x._1+"\t"+x._2._1+"\t"+x._2._2
    })



    //------------------------------------------------------------------------------------------------------

    val idfaCountPerIp = appleUsers.map(x=>{
      val ip = x.getString(x.fieldIndex("ip"))
      val idfa = x.getString(x.fieldIndex("idfa"))
      ip+"\t"+idfa
    }).distinct.map(x=>{
      val ip = x.split("\t")(0)
      (ip,1)
    }).reduceByKey(_+_)

    val ipDayMap = appleUsers.map(x=>{
      val ip = x.getString(x.fieldIndex("ip"))
      val day = x.getString(x.fieldIndex("day"))
      (ip+"\t"+day,1)
    }).reduceByKey(_+_)

    val vCountPerIp = ipDayMap.map(x=>{
      val ip = x._1.split("\t")(0)
      (ip,x._2)
    }).reduceByKey(_+_)

    val vCountMax = ipDayMap.map(x=>{
      val ip = x._1.split("\t")(0)
      val count = x._2
      (ip,count)
    }).reduceByKey((x,y)=>{
      Math.max(x,y)
    })

    val days = appleUsers.map(x=>{
      val ip = x.getString(x.fieldIndex("ip"))
      val day = x.getString(x.fieldIndex("day"))
      ip+"\t"+day
    }).distinct.map(x=>{
      val ip = x.split("\t")(0)
      (ip,1)
    }).reduceByKey(_+_)

    val vCountPerDay = vCountPerIp.join(days).map(x=>{
      val ip = x._1
      val mean = (x._2._1.toDouble/x._2._2.toDouble).formatted("%.2f")
      (ip,mean)
    })

    val hours = appleUsers.map(x=>{
      val ip = x.getString(x.fieldIndex("ip"))
      val hour = x.getString(x.fieldIndex("hour"))
      (ip,Math.pow(2,hour.toDouble).toInt)
    }).distinct.reduceByKey(_+_)

    val result = idfaCountPerIp.join(vCountPerIp).map(x=>{
      (x._1,x._2._1+"\t"+x._2._2)
    }).join(vCountMax).map(x=>{
      (x._1,x._2._1+"\t"+x._2._2)
    }).join(days).map(x=>{
      (x._1,x._2._1+"\t"+x._2._2)
    }).join(vCountPerDay).map(x=>{
      (x._1,x._2._1+"\t"+x._2._2)
    }).join(hours).map(x=>{
      x._1+"\t"+x._2._1+"\t"+x._2._2
    })


    val idfaPath = outPathPre+"/idfa/"+begain+"to"+end

    AnomalyUtil.existsPath(idfaPath)

    info.coalesce(
      if(info.partitions.size>part) info.partitions.size/part
      else 1
    ).saveAsTextFile(idfaPath,classOf[GzipCodec])

    val ipPath = outPathPre+"/ip/"+begain+"to"+end

    AnomalyUtil.existsPath(ipPath)

    result.coalesce(
      if(result.partitions.length>part) result.partitions.length/part
      else 1
    ).saveAsTextFile(ipPath,classOf[GzipCodec])

    sqlContext.sql("use "+database)
    sqlContext.sql("alter table idfa_status drop if exists partition (d="+end+")")
    sqlContext.sql("alter table idfa_status add partition(d="+end+") location '"+idfaPath+"'")
    sqlContext.sql("alter table ip_status drop if exists partition (d="+end+")")
    sqlContext.sql("alter table ip_status add partition(d="+end+") location '"+ipPath+"'")

    sc.stop()
  }
}
