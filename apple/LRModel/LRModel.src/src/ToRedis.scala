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
    //得到所有的权重
    val weights = sqlContext.sql("select weights from lr").rdd.collect().mkString.replace("[[", "").replace("]]", "").split(",")

    //得到所有的特征
    val features = sc.textFile(args(1)).collect()

    //特征和index的对应
    val featureAndIndexMap = scala.collection.mutable.Map[String, String]()
    for (i <- features) yield {
      featureAndIndexMap += (i.split(",")(1) -> i.split(",")(0))
    }

    //放到broadcast中
    val maps = sc.broadcast(featureAndIndexMap)

    val weightsIndex = sc.accumulator(0)

    //最终结果  需要index和对应的权重

    val results = for(i <- 0 until weights.length) yield {
      weights(i)+","+maps.value((i+1).toString)
    }

    val resultrdd = sc.makeRDD(results)

    val tt = sc.accumulator(0)
    resultrdd.foreachPartition { x =>
      val pair = new java.util.HashMap[String,String]()
      val jedis = new Jedis(args(2),args(3).toInt)
      x.foreach { y =>
        pair.put(y.split(",")(1), y.split(",")(0))
    }
      jedis.hmset(args(4)+"_weights", pair)
    }

    val feature = sc.textFile(args(5))
    feature.foreachPartition(x=>{
      x.foreach(y=>{
        val jedis = new Jedis(args(2),args(3).toInt)
        val pair = new util.HashMap[String,String]()
        pair.put(y.split(",")(0), y.split(",")(1))
        jedis.hmset(args(4)+"_feature",pair)
      })
    })

    sc.stop()
  }
}
