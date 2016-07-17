import java.util

import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
 * Created by hadoop on 2016/1/15.
 *
 *
 * args（0）模型的路径
 * args（1）feature的路径
 * args（2）host
 * args（3）port
 * args (4) 模型编号
 */
object ToRedis {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    val sc = new SparkContext( sparkConf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")

    val parquetFile = sqlContext.read.parquet(args(0)+"/model/data")
    parquetFile.registerTempTable("lr")
    val weights = sqlContext.sql("select weights from lr").rdd.collect().mkString.replace("[[", "").replace("]]", "").split(",")

    val features = sc.textFile(args(1)).collect()
    val featureAndIndexMap = scala.collection.mutable.Map[String, String]()
    for (i <- features) yield {
      featureAndIndexMap += (i.split(",")(1) -> i.split(",")(0))
    }

    val maps = sc.broadcast(featureAndIndexMap)

    val results = for(i<-weights) yield{
      i+","+ maps.value((weights.indexOf(i)+1).toString())
    }

    val resultrdd = sc.makeRDD(results)
    resultrdd.foreachPartition { x => x.foreach { y =>
      val jedis = new Jedis(args(2),args(3).toInt)
//      val jedis = new Jedis("123.59.25.137",7000)
      val pair = new util.HashMap[String,String]()
      pair.put(y.split(",")(1), y.split(",")(0))
      jedis.hmset(args(4), pair)
    } }
    sc.stop()
  }
}
