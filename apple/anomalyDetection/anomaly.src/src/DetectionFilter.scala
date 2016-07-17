import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by hadoop on 2016/3/9.
 */
object DetectionFilter {
  def main(args: Array[String]) {
    val idfaFilter = args(0).toDouble
    val ipFilter = args(1).toDouble
    val day = args(2)
    val part = args(3).toInt
    val outPathPre = args(4)

    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use formatlog")
    val df = sqlContext.sql("select * from detection where d='"+day+"' and target='idfa'").map(x=>{
      val idfa = x.getString(x.fieldIndex("name"))
      val prop = x.getString(x.fieldIndex("probability")).toDouble
      (idfa,prop)
    }).filter(_._2<idfaFilter).map(x=>x._1)

    val outPath = outPathPre+"/idfa/"+day
    DetecUtil.pathExists(outPath)
    df.coalesce(part).saveAsTextFile(outPath)

    val ipOutPath = outPathPre+"/ip/"+day
    val ipDf = sqlContext.sql("select * from detection where d='"+day+"' and target='ip'").map(x=>{
      val ip = x.getString(x.fieldIndex("name"))
      val prop = x.getString(x.fieldIndex("probability")).toDouble
      (ip,prop)
    }).filter(_._2<ipFilter).map(x=>"blk:"+x._1)
    DetecUtil.pathExists(ipOutPath)
    ipDf.coalesce(part).saveAsTextFile(ipOutPath)
    sc.stop()
  }
}
