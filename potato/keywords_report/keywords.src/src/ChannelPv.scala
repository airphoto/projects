import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hadoop on 2016/5/20.
 */
object ChannelPv {
  def main(args: Array[String]) {

    if(args.length<3){
      println("usage : ChannelPv <keywordPath> <day> <outPathPre>")
      System.exit(1)
    }

    @transient val sparkConf = new SparkConf().setAppName("channelPV")
    @transient val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val keywordPath = args(0)///test/work/keywords/keywordsChannel_20160523
    val day = args(1)
    val outPathPre = args(2)

    sqlContext.sql("use formatlog")

    //词包文件
    val keywordsFile = sc.textFile(keywordPath)

    //词包转化成Set
    @transient val keywordSet = keywordsFile.map(x=>(x.split("\t")(0),None)).collectAsMap().keySet

    //bidder数据
    val bidData = sqlContext.table("bid_request").where("d="+day).select("keywords","os","channel")

    //统计每个频道中每个关键词的pv
    val channelMaps = bidData.flatMap(x=>{
      val keywords = x.getList[String](x.fieldIndex("keywords"))
      val os = x.getString(x.fieldIndex("os")).toLowerCase
      val system = if(os.contains("ios")) "IOS" else if(os.contains("android")) "Android" else "-"
      val channel = x.getString(x.fieldIndex("channel"))

      for(i<-0 until keywords.size) yield {
        if(keywords.get(i) != "" && channel!="" && keywordSet.contains(keywords.get(i))) (channel +"\t"+ keywords.get(i)+"->"+system,1)
        else("",1)
      }
    }).filter(x=>x._1!="")

    //将相同系统下的相同的channel出现次数相加
    val reduceData = channelMaps.reduceByKey(_+_)
    reduceData.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)

    val channelIOS = reduceData.filter(x=>(x._1.contains("IOS"))).map(x=>{
      val name = x._1.split("->")(0)
      (name,x._2)
    })
    val channelAndroid = reduceData.filter(x=>(x._1.contains("Android"))).map(x=>{
      val name = x._1.split("->")(0)
      (name,x._2)
    })

    val IOSOutPath = outPathPre+"/IOS/"+day
    val AndroidOutPath = outPathPre+"Android/"+day

    //在hdfs中选在指定的路径就删除
    existsPath(IOSOutPath)
    existsPath(AndroidOutPath)

    //保存数据
    channelIOS.coalesce(1).saveAsTextFile(IOSOutPath)
    channelAndroid.coalesce(1).saveAsTextFile(AndroidOutPath)

    sc.stop()
  }

  def existsPath(path:String): Unit ={
    val conf = new Configuration()
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val hdfsPath = new Path(path)
    if(fs.exists(hdfsPath)) fs.delete(hdfsPath,true)
  }
}
