import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/* @author hadoop
 * 统计特征,加上前缀,编号,存储
 * 
 * args(0) 存储着列名和其简写的文件路径
 * args(1) 存储特征和其对应的index的路径
 * args(2) 统计结束的时间
 */
object Features {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    sqlContext.sql("use formatlog")//选取数据库

    val caslPath = args(0)//存储列名和其简写的文件路径
    val outPath = args(1)//存储特征和其对应的index的路径，index从1开始
    val dayCondition = if(args.length==3) " and d<="+args(2) else ""//统计结束的时间,默认是到最后一天
    val columnAndShortList = sc.textFile(caslPath).collect().toList//获取存储列名和其简写的文件，文件中每一行都用逗号隔开，逗号前是全名，后是缩写名称
    val array = scala.collection.mutable.ArrayBuffer[String]()//总特征值列表

     for (i <- columnAndShortList) {//获取所需列的特征值
      val column = i.split(",")(0)//列全名
      val short = i.split(",")(1)//缩写名
      val frame = sqlContext.sql("select distinct(" + column + ") from deliveries where " + column + " is not null and d>=20151229"+dayCondition)
       array ++= frame.map { x => short + x.mkString }.collect()//将获取的特征值（前缀+特征值）添加到总特征值列表中
    }

    val conf = new org.apache.hadoop.conf.Configuration()
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val hdfsOutPath = new org.apache.hadoop.fs.Path(outPath)
    if (fs.exists(hdfsOutPath)) fs.delete(hdfsOutPath, true)//如果存储路径已经存在就删除

    val tmp = for (i <- array) yield {
      i+","+(array.indexOf(i) + 1)
    }//存储特征值和对应的index  如  sc1113322，234

    sc.makeRDD(tmp).coalesce(1).saveAsTextFile(outPath)//合并到一个文件中存储起来

    sc.stop()
  }
}