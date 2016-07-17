import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by hadoop on 2016/3/2.
 */
object Filter{
  def main(args: Array[String]) {
    if(args.length<6){
      println("outPathPre, day, par, ipDeviceFilter, ipVideoFilter, idfaVideoFilter")
      System.exit(-1)
    }
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val Array(outPathPre, day, par, ipDeviceFilter, ipVideoFilter, idfaVideoFilter) = args

    sqlContext.sql("use formatlog")
    val ip = sqlContext.sql("select ip,vcount,idfacount from ip_status where d='" + day + "'")
    val ipFilterData = ip.filter(ip("vcount") > ipVideoFilter.toInt || ip("idfacount") > ipDeviceFilter.toInt)
    val ipForRedis = ipFilterData.map(x => {
      "blk:"+x.getString(x.fieldIndex("ip"))
    })
    val ipForHive = ipFilterData.map(x => {
      val ip = x.getString(x.fieldIndex("ip"))
      val videoCount = x.getString(x.fieldIndex("vcount"))
      val deviceCount = x.getString(x.fieldIndex("idfacount"))
      "blk:"+ip + "\t" + videoCount + "\t" + deviceCount
    })
    val idfa = sqlContext.sql("select idfa,vcount from idfa_status where d='" + day + "'")
    val idfaFilterData = idfa.filter(idfa("vcount") > idfaVideoFilter.toInt)
    val idfaForRedis = idfaFilterData.map(x => {
      x.getString(x.fieldIndex("idfa"))
    })
    val idfaForHive = idfaFilterData.map(x => {
      x.getString(x.fieldIndex("idfa")) + "\t" + x.getString(x.fieldIndex("vcount"))
    })
    val ipOutPath = outPathPre + "/ip/" + day
    val idfaOutPath = outPathPre + "/idfa/" + day
    val ipForHivePath = outPathPre + "/hiveIp/" + day
    val idfaForHivePath = outPathPre + "/hiveIdfa/" + day

    AnomalyUtil.existsPath(ipOutPath)
    AnomalyUtil.existsPath(idfaOutPath)
    AnomalyUtil.existsPath(ipForHivePath)
    AnomalyUtil.existsPath(idfaForHivePath)


    idfaForRedis.coalesce(par.toInt).saveAsTextFile(idfaOutPath)
    ipForRedis.coalesce(par.toInt).saveAsTextFile(ipOutPath)
    idfaForHive.saveAsTextFile(idfaForHivePath, classOf[GzipCodec])
    ipForHive.saveAsTextFile(ipForHivePath, classOf[GzipCodec])

    sqlContext.sql("alter table idfa_filter drop if exists partition (d="+day+")")
    sqlContext.sql("alter table idfa_filter add partition(d="+day+") location '"+idfaForHivePath+"'")
    sqlContext.sql("alter table ip_filter drop if exists partition (d="+day+")")
    sqlContext.sql("alter table ip_filter add partition(d="+day+") location '"+ipForHivePath+"'")
    sc.stop()
  }
}
