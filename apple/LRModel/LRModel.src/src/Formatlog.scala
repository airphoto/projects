

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import jodd.util.StringBand
/* @author hadoop
 * 格式化数据    如  1 3：1 33：1 205：1
 * args(0) //存储列名和其对缩写的文件的路径
   args(1) //特征和其对应的index的文件的路径
   args(2) //存储格式化数据的路径
   args(3) //如果没有args(4)，它就是结束时间，否则就是开始时间
   args(4) //结束日期
 */
object Formatlog {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    sqlContext.sql("use formatlog")

    val caslPath = args(0) //存储列名和其对缩写的文件的路径
    val failPath = args(1) //特征和其对应的index的文件的路径
    val outPath = args(2) //存储格式化数据的路径

    val columnAndShortList = sc.textFile(caslPath).collect().toList;
    val columnShortMap = scala.collection.mutable.Map[String, String]()
    for (i <- columnAndShortList) {
      columnShortMap += (i.split(",")(0) -> i.split(",")(1))
    }

    val columns = for (i <- columnAndShortList) yield i.split(",")(0) //需要的列名

    val sb = StringBuilder.newBuilder
    for (i <- columns) {
      sb.append(i).append(",")
    }

    val dayCondition = if(args.length==3) " and d>=20151229" else if(args.length==4) " and d>=20151229 and d<="+args(3) else if(args.length==5) " and d>="+args(3)+" and d<="+args(4)//统计结束的时间,默认是到最后一天
    val last = if(args.length==3) " d>=20151229" else if(args.length==4) " d>=20151229 and d<="+args(3) else if(args.length==5) " d>="+args(3)+" and d<="+args(4)//统计结束的时间,默认是到最后一天

    val delSql = "select " + sb.toString() + "click_uuid from deliveries where request_time is not null" + dayCondition
    val conSql = "select format_uuid from conversions where" + last

    val del = sqlContext.sql(delSql)
    val con = sqlContext.sql(conSql)

    val csmBroad = sc.broadcast(columnShortMap) //全名-》简写名称

    val featureAndIndexList = sc.textFile(failPath).collect().toList
    val featureIndexMap = scala.collection.mutable.Map[String, String]()
    for (i <- featureAndIndexList) {
      featureIndexMap += (i.split(",")(0) -> i.split(",")(1))
    }

    val fimBroad = sc.broadcast(featureIndexMap) //特征值和其对应的index

    import org.apache.spark.sql.functions._//隐式转换scala
    val table = del.join(con, del("click_uuid").equalTo(con("format_uuid")), "left_outer") //以deliveries为准的join

    /*
     * 除去click_uuid
     */
    val lines = table.drop("click_uuid").map { x =>
      val sb = StringBuilder.newBuilder
      val exp = if (x.isNullAt(x.fieldIndex("format_uuid"))) 0 else 1//如果format_uuid为空，说明没有转化，为0
      for (i <- columns) {
        val short = csmBroad.value(i)//前缀
        val fim = fimBroad.value//特征值（带前缀）和其对应的index的map
        val tmp = if (fim.keySet.contains(short +"_"+ x.get(x.fieldIndex(i)))) " " + fim(short +"_"+ x.get(x.fieldIndex(i))) + ":1" else ""
        sb.append(tmp)
      }
      exp + sb.toString()
    }

    val conf = new org.apache.hadoop.conf.Configuration()
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val hdfsOutPath = new org.apache.hadoop.fs.Path(outPath)
    if (fs.exists(hdfsOutPath)) fs.delete(hdfsOutPath, true)//如果存储路径已经存在就删除

   lines.coalesce(50).saveAsTextFile(outPath)

   sc.stop()
  }
}