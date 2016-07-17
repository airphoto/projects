import java.sql.{PreparedStatement, Connection}
import java.util.Properties

import com.hq.now.DataSourceFactory
import org.apache.spark.{SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{StreamingContext, Seconds}

/**
  * Created by hadoop on 2016/6/12.
  */
object KafkaWordCount {
  def main(args: Array[String]) {

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.checkpoint("KafkaWordCounts")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).
      reduceByKeyAndWindow(_ + _, _ - _, Seconds(20), Seconds(10), 2)

    wordCounts.print()
/*
    wordCounts.foreachRDD(rdd=>{

      val insert = "insert into teset(name,counts) values(?,?)"
      val update = "update teset set counts=? where name=?"

      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)

      val prop = new Properties()
      prop.put("user","persim")
      prop.put("password","Hq6Rr3MJwv3rAPerSi")
      prop.put("tinyInt1isBit","false")

      val mysqlData = sqlContext.read.jdbc("jdbc:mysql://123.59.25.238/test","teset",prop)
      val mysqlTuple = mysqlData.map(x=>{
        val word = x.getString(x.fieldIndex("name"))
        val count = x.getInt(x.fieldIndex("counts"))
        (word,count)
      })

      val joinData = mysqlTuple.fullOuterJoin(rdd).map(x=>{
        val word = x._1
        val count1 = x._2._1
        val count2 = x._2._2
        (word,count1,count2)
      })

      val updateData = joinData.filter(x=>(!x._2.isEmpty && !x._3.isEmpty))
      val newData = joinData.filter(x=>x._2.isEmpty)

      updateData.foreachPartition(x=>{
        var conn:Connection = null
        var ps:PreparedStatement = null
        try {
          conn = DataSourceFactory.getConnection
          x.foreach(y => {
            ps = conn.prepareStatement(update)
            ps.setString(2,y._1)
            ps.setLong(1,y._2.get+y._3.get)
            ps.executeUpdate()
          })
        }catch {
          case e:Exception=>e.printStackTrace()
        }finally {
          DataSourceFactory.closeCon(null,ps,conn)
        }
      })

      newData.foreachPartition(x=>{
        var conn:Connection = null
        var ps:PreparedStatement = null
        try {
          conn = DataSourceFactory.getConnection
          x.foreach(y => {
            ps = conn.prepareStatement(insert)
            ps.setString(1,y._1)
            ps.setLong(2,y._3.get)
            ps.execute()
          })
        }catch {
          case e:Exception=>e.printStackTrace()
        }finally {
          DataSourceFactory.closeCon(null,ps,conn)
        }
      })
    })*/
    ssc.start()
    ssc.awaitTermination()
  }
}
