import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by hadoop on 2016/3/7.
 */
object Detection {
  case class Diff_2(ipcount:Double,vcount:Double,ipmax:Double,vcountmax:Double,vdays:Double,vdaymean:Double,vipmean:Double,hours:Double,ipdaymean:Double)
  case class IpDiff_2(idfacount:Double,vcount:Double,vmax:Double,vdays:Double,vdaymean:Double,hours:Double)
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    val sqlContext = new HiveContext(sc)

    val targetTime = args(0)
    val database = args(1)
    val outPathPre = args(2)
    val parts = args(3).trim.toInt

    sqlContext.sql("use "+database)
//-----idfa算法实现----------
    val df = sqlContext.sql("select * from idfa_status where d='"+targetTime+"'").toDF()

    val userCount = df.count()

    df.registerTempTable("idfa")

    val miu = scala.collection.mutable.Map[String,Double]()//每一列的平均值

    sqlContext.sql("select avg(ipcount) as ipmean,avg(vcount) as vmean,avg(ipmax) as ipmaxmean,avg(vcountmax) as vmaxmean,avg(vdays) as daysmean,avg(vdaymean) as vdaymean,avg(vipmean) as vipmean,avg(time) as hours,avg(ipdaymean) as ipdaymean from idfa_status where d='"+targetTime+"'").collect().foreach(x=>{
      miu += ("ipcount"->x.getDouble(x.fieldIndex("ipmean")))
      miu += ("vcount"->x.getDouble(x.fieldIndex("vmean")))
      miu += ("ipmax"->x.getDouble(x.fieldIndex("ipmaxmean")))
      miu += ("vcountmax"->x.getDouble(x.fieldIndex("vmaxmean")))
      miu += ("vdays"->x.getDouble(x.fieldIndex("daysmean")))
      miu += ("vdaymean"->x.getDouble(x.fieldIndex("vdaymean")))
      miu += ("vipmean"->x.getDouble(x.fieldIndex("vipmean")))
      miu += ("hours"->x.getDouble(x.fieldIndex("hours")))
      miu += ("ipdaymean"->x.getDouble(x.fieldIndex("ipdaymean")))
    })


    import sqlContext.implicits._
    //每个字段距离平均值距离的平方
    val squares = df.map(x=>{
      val ips = DetecUtil.getDiff_2(x.getString(x.fieldIndex("ipcount")).toDouble,miu("ipcount"))
      val vs = DetecUtil.getDiff_2(x.getString(x.fieldIndex("vcount")).toDouble,miu("vcount"))
      val ipmax = DetecUtil.getDiff_2(x.getString(x.fieldIndex("ipmax")).toDouble,miu("ipmax"))
      val vmax = DetecUtil.getDiff_2(x.getString(x.fieldIndex("vcountmax")).toDouble,miu("vcountmax"))
      val vdays = DetecUtil.getDiff_2(x.getString(x.fieldIndex("vdays")).toDouble,miu("vdays"))
      val vdaymean = DetecUtil.getDiff_2(x.getString(x.fieldIndex("vdaymean")).toDouble,miu("vdaymean"))
      val vipmean = DetecUtil.getDiff_2(x.getString(x.fieldIndex("vipmean")).toDouble,miu("vipmean"))
      val hours = DetecUtil.getDiff_2(x.getString(x.fieldIndex("time")).toDouble,miu("hours"))
      val ipdaymean = DetecUtil.getDiff_2(x.getString(x.fieldIndex("ipdaymean")).toDouble,miu("ipdaymean"))
      Diff_2(ips,vs,ipmax,vmax,vdays,vdaymean,vipmean,hours,ipdaymean)
    }).toDF()

    val sigma_2 = scala.collection.mutable.Map[String,Double]()//每一列的方差
    squares.registerTempTable("square")
    sqlContext.sql("select sum(ipcount) as ipSq,sum(vcount) as vSq,sum(ipmax) as ipmaxSq,sum(vcountmax) as vmaxSq,sum(vdays) as vdSq,sum(vdaymean) as vdmSq,sum(vipmean) as vimSq,sum(hours) as hours,sum(ipdaymean) as ipdaymean from square").collect.foreach(x=>{
      sigma_2+=("ipcount"->(x.getDouble(x.fieldIndex("ipSq"))/userCount))
      sigma_2+=("vcount"->(x.getDouble(x.fieldIndex("vSq"))/userCount))
      sigma_2+=("ipmax"->(x.getDouble(x.fieldIndex("ipmaxSq"))/userCount))
      sigma_2+=("vcountmax"->(x.getDouble(x.fieldIndex("vmaxSq"))/userCount))
      sigma_2+=("vdays"->(x.getDouble(x.fieldIndex("vdSq"))/userCount))
      sigma_2+=("vdaymean"->(x.getDouble(x.fieldIndex("vdmSq"))/userCount))
      sigma_2+=("vipmean"->(x.getDouble(x.fieldIndex("vimSq"))/userCount))
      sigma_2+=("hours"->(x.getDouble(x.fieldIndex("hours"))/userCount))
      sigma_2+=("ipdaymean"->(x.getDouble(x.fieldIndex("ipdaymean"))/userCount))
    })


    val px = df.map(x=>{
      //每一行中每个字段的概率，然后同一列概率相乘得到最终结果
      val idfa = x.getString(x.fieldIndex("idfa"))
      //val tmp = (1/(Math.pow(Math.PI*2,0.5)*Math.pow(sigma_2("ipcount"),0.5)))*Math.pow(Math.E,(-(Math.pow((x.fieldIndex("ipcount").toDouble-miu("ipcount")),2)/(2*sigma_2("ipcount")))))
      val pxIp = DetecUtil.getResult(x.getString(x.fieldIndex("ipcount")).toDouble,sigma_2("ipcount"),miu("ipcount"))
      val pxVcount = DetecUtil.getResult(x.getString(x.fieldIndex("vcount")).toDouble,sigma_2("vcount"),miu("vcount"))
      val pxIpMax = DetecUtil.getResult(x.getString(x.fieldIndex("ipmax")).toDouble,sigma_2("ipmax"),miu("ipmax"))
      val pxVmax = DetecUtil.getResult(x.getString(x.fieldIndex("vcountmax")).toDouble,sigma_2("vcountmax"),miu("vcountmax"))
      val pxVdays = DetecUtil.getResult(x.getString(x.fieldIndex("vdays")).toDouble,sigma_2("vdays"),miu("vdays"))
      val pxVdaymean = DetecUtil.getResult(x.getString(x.fieldIndex("vdaymean")).toDouble,sigma_2("vdaymean"),miu("vdaymean"))
      val pxVipmean = DetecUtil.getResult(x.getString(x.fieldIndex("vipmean")).toDouble,sigma_2("vipmean"),miu("vipmean"))
      val pxHours = DetecUtil.getResult(x.getString(x.fieldIndex("time")).toDouble,sigma_2("hours"),miu("hours"))
      val pxipdmean = DetecUtil.getResult(x.getString(x.fieldIndex("ipdaymean")).toDouble,sigma_2("ipdaymean"),miu("ipdaymean"))

//      idfa+"\t"+(pxIp*pxVcount*pxIpMax*pxVmax*pxVdays*pxVdaymean*pxVipmean*pxHours*pxipdmean)
//      idfa+"\t"+tmp
      idfa+"\t"+pxIp+"\t"+pxVcount+"\t"+pxIpMax+"\t"+pxVmax+"\t"+pxVdays+"\t"+pxVdaymean+"\t"+pxVipmean+"\t"+pxHours+"\t"+pxipdmean//+(pxIp*pxVcount*pxIpMax*pxVmax*pxVdays*pxVdaymean*pxVipmean)
    })

    //---将概率分段，然后统计每一段的人数-------------------------------------
    val stat = px.map(x=>{
      x.split("\t")(1).toDouble//获取概率
    })

    val max = stat.max()//找出概率的最大值

    val pxArr = 0.0.to(max,max/parts).toBuffer//将0.0到max之间的数分成等差数列，差值为diff
    val pxMap = scala.collection.mutable.Map[Int,Double]()//等差段与其index的map
    if(pxArr.last!=max) pxArr+=max
    for(i<-1 until pxArr.length){
      pxMap+=(i->pxArr(i))
    }

    //计算每个分段的人数
    val partCount = stat.map(x=>{
      (pxMap(DetecUtil.getIndex(x,pxArr.toArray,0,pxArr.length-1)),1)
    }).reduceByKey(_+_).sortBy(x=>x._1,false)

    val sectionsOutPath = outPathPre+"/section/idfa/"+targetTime
    DetecUtil.pathExists(sectionsOutPath)
    partCount.map(x=>{
      x._1+"\t"+x._2
    }).coalesce(1).saveAsTextFile(sectionsOutPath)

    //---统计人数------

    val outPath = outPathPre+"/detection/idfa/"+targetTime
    DetecUtil.pathExists(outPath)
    px.coalesce(10).saveAsTextFile(outPath,classOf[GzipCodec])

    sqlContext.sql("alter table sections drop if exists partition(d='"+targetTime+"',target='idfa')")
    sqlContext.sql("alter table sections add partition(d='"+targetTime+"',target='idfa') location '"+sectionsOutPath+"'")

    sqlContext.sql("alter table detection drop if exists partition(d='"+targetTime+"',target='idfa')")
    sqlContext.sql("alter table detection add partition(d='"+targetTime+"',target='idfa') location '"+outPath+"'")
//-----idfa算法实现----------------------------------------------

//-----ip算法实现----------------------------------------------

    val ipDf = sqlContext.sql("select * from ip_status where d='"+targetTime+"'").toDF()
    val ipCount = ipDf.count()

    ipDf.registerTempTable("ips")

    val ipMiu = scala.collection.mutable.Map[String,Double]()//每一列的平均值

    sqlContext.sql("select avg(idfacount) as idfacount,avg(vcount) as vcount,avg(vcountmax) as vcountmax,avg(vdays) as vdays,avg(vdaymean) as vdaymean,avg(time) as hours from ip_status where d='"+targetTime+"'").collect().foreach(x=>{
      ipMiu += ("idfacount"->x.getDouble(x.fieldIndex("idfacount")))
      ipMiu += ("vcount"->x.getDouble(x.fieldIndex("vcount")))
      ipMiu += ("vcountmax"->x.getDouble(x.fieldIndex("vcountmax")))
      ipMiu += ("vdays"->x.getDouble(x.fieldIndex("vdays")))
      ipMiu += ("vdaymean"->x.getDouble(x.fieldIndex("vdaymean")))
      ipMiu += ("hours"->x.getDouble(x.fieldIndex("hours")))
    })
//case class IpDiff_2(idfacount:Double,vcount:Double,vmax:Double,vdays:Double,vdaymean:Double,hours:Double)

    //每个字段距离平均值距离的平方
    val ipDiff2 = ipDf.map(x=>{
      val idfas = DetecUtil.getDiff_2(x.getString(x.fieldIndex("idfacount")).toDouble,ipMiu("idfacount"))
      val vs = DetecUtil.getDiff_2(x.getString(x.fieldIndex("vcount")).toDouble,ipMiu("vcount"))
      val vmax = DetecUtil.getDiff_2(x.getString(x.fieldIndex("vcountmax")).toDouble,ipMiu("vcountmax"))
      val vdays = DetecUtil.getDiff_2(x.getString(x.fieldIndex("vdays")).toDouble,ipMiu("vdays"))
      val vdaymean = DetecUtil.getDiff_2(x.getString(x.fieldIndex("vdaymean")).toDouble,ipMiu("vdaymean"))
      val hours = DetecUtil.getDiff_2(x.getString(x.fieldIndex("time")).toDouble,ipMiu("hours"))
      IpDiff_2(idfas,vs,vmax,vdays,vdaymean,hours)
    }).toDF()

//IpDiff_2(idfacount:Double,vcount:Double,vmax:Double,vdays:Double,vdaymean:Double,hours:Double)
    val ipSigma_2 = scala.collection.mutable.Map[String,Double]()//每一列的方差
    ipDiff2.registerTempTable("ipdiff")
    sqlContext.sql("select sum(idfacount) as idfacount,sum(vcount) as vcount,sum(vmax) as vmax,sum(vdays) as vdSq,sum(vdaymean) as vdmSq,sum(hours) as hours from ipdiff").collect.foreach(x=>{
      ipSigma_2+=("idfacount"->(x.getDouble(x.fieldIndex("idfacount"))/ipCount))
      ipSigma_2+=("vcount"->(x.getDouble(x.fieldIndex("vcount"))/ipCount))
      ipSigma_2+=("vcountmax"->(x.getDouble(x.fieldIndex("vmax"))/ipCount))
      ipSigma_2+=("vdays"->(x.getDouble(x.fieldIndex("vdSq"))/ipCount))
      ipSigma_2+=("vdaymean"->(x.getDouble(x.fieldIndex("vdmSq"))/ipCount))
      ipSigma_2+=("hours"->(x.getDouble(x.fieldIndex("hours"))/ipCount))
    })

    val ipResult = ipDf.map(x=>{
      //每一行中每个字段的概率，然后同一列概率相乘得到最终结果
      val ip = x.getString(x.fieldIndex("ip"))
      val idfacount = DetecUtil.getResult(x.getString(x.fieldIndex("idfacount")).toDouble,ipSigma_2("idfacount"),ipMiu("idfacount"))
      val pxVcount = DetecUtil.getResult(x.getString(x.fieldIndex("vcount")).toDouble,ipSigma_2("vcount"),ipMiu("vcount"))
      val pxVmax = DetecUtil.getResult(x.getString(x.fieldIndex("vcountmax")).toDouble,ipSigma_2("vcountmax"),ipMiu("vcountmax"))
      val pxVdays = DetecUtil.getResult(x.getString(x.fieldIndex("vdays")).toDouble,ipSigma_2("vdays"),ipMiu("vdays"))
      val pxVdaymean = DetecUtil.getResult(x.getString(x.fieldIndex("vdaymean")).toDouble,ipSigma_2("vdaymean"),ipMiu("vdaymean"))
      val pxHours = DetecUtil.getResult(x.getString(x.fieldIndex("time")).toDouble,ipSigma_2("hours"),ipMiu("hours"))
      ip+"\t"+(idfacount*pxVcount*pxVmax*pxVdays*pxVdaymean*pxHours)
      //      idfa+"\t"+pxIp+"\t"+pxVcount+"\t"+pxIpMax+"\t"+pxVmax+"\t"+pxVdays+"\t"+pxVdaymean+"\t"+pxVipmean+"\t"+(pxIp*pxVcount*pxIpMax*pxVmax*pxVdays*pxVdaymean*pxVipmean)
    })
//--------------------
val ipStat = ipResult.map(x=>{
  x.split("\t")(1).toDouble//获取概率
})

    val ipmax = ipStat.max()//找出概率的最大值

    val ipArr = 0.0.to(ipmax,ipmax/parts).toBuffer//将0.0到max之间的数分成等差数列，差值为diff
    val ipMap = scala.collection.mutable.Map[Int,Double]()//等差段与其index的map
    if(ipArr.last!=ipmax) ipArr+=ipmax
    for(i<-1 until ipArr.length){
      ipMap+=(i->ipArr(i))
    }

    //计算每个分段的人数
    val sectionCount = ipStat.map(x=>{
      (ipMap(DetecUtil.getIndex(x,ipArr.toArray,0,ipArr.length-1)),1)
    }).reduceByKey(_+_).sortBy(x=>x._1,false)

    val ipSectionsOutPath = outPathPre+"/section/ip/"+targetTime
    DetecUtil.pathExists(ipSectionsOutPath)
    sectionCount.map(x=>{
      x._1+"\t"+x._2
    }).coalesce(1).saveAsTextFile(ipSectionsOutPath)

    val ipOutPath = outPathPre+"/detection/ip/"+targetTime
    DetecUtil.pathExists(ipOutPath)
    ipResult.coalesce(10).saveAsTextFile(ipOutPath,classOf[GzipCodec])

    sqlContext.sql("alter table sections drop if exists partition(d='"+targetTime+"',target='ip')")
    sqlContext.sql("alter table sections add partition(d='"+targetTime+"',target='ip') location '"+ipSectionsOutPath+"'")

    sqlContext.sql("alter table detection drop if exists partition(d='"+targetTime+"',target='ip')")
    sqlContext.sql("alter table detection add partition(d='"+targetTime+"',target='ip') location '"+ipOutPath+"'")

    sc.stop()
  }
}
