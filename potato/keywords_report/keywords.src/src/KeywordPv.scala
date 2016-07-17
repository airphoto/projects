import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by hadoop on 2016/5/20.
 */
object KeywordPv {
  def main(args: Array[String]) {

    if(args.length<3){
      println("usage : KeywordPv <keywordFilePath> <targetDay> <outPathPre>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("keywordPV")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val Array(keywordFilePath,targetDay,outPathPre) = args

    sqlContext.sql("use formatlog")

    //关键词包文件
    val keywordFile = sc.textFile(keywordFilePath)
    //将关键词转化成set
    val keywordSet = keywordFile.map(x=>(x,None)).collectAsMap().keySet

    //获取bidder的数据
    val bidData = sqlContext.table("bid_request").where("d="+targetDay).select("keywords","os")

    //每个keywords数组中与关键词包的set做对比，数组中出现的词都标志 1 和 相应的系统
    val systemAndKeyword = bidData.flatMap(x=>{
      val keywords = x.getList[String](x.fieldIndex("keywords"))
      val os = x.getString(x.fieldIndex("os")).toLowerCase
      val system = if(os.contains("ios")) "IOS" else if(os.contains("android")) "Android" else "-"
      for(i<- 0 until keywords.size) yield {
        if(keywordSet.contains(keywords.get(i))) (system+"\t"+keywords.get(i),1)
        else ("",1)
      }
    }).filter(x=>{
      x._1!="" && x._1.split("\t")(0)!="-"
    })

    //将相同系统下的相同关键词出现的次数相加
    val reduceData = systemAndKeyword.reduceByKey(_+_)
    reduceData.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)

    //IOS的数据
    val dataIOS = reduceData.filter(x=>(x._1.contains("IOS"))).map(x=>{
      val name = x._1.split("\t")(1)
      (name,x._2)
    })

    //Android的数据
    val dataAndroid = reduceData.filter(x=>(x._1.contains("Android"))).map(x=>{
      val name = x._1.split("\t")(1)
      (name,x._2)
    })

    val IOSOutPath = outPathPre+"/IOS/"+targetDay
    val AndroidOutPath = outPathPre+"/Android/"+targetDay

    //若HDFS中存在相应的路径就删除
    existsPath(IOSOutPath)
    existsPath(AndroidOutPath)

    //存储数据
    dataIOS.coalesce(1).saveAsTextFile(IOSOutPath)
    dataAndroid.coalesce(1).saveAsTextFile(AndroidOutPath)

    sc.stop()
  }

  def existsPath(path:String): Unit ={
    val conf = new Configuration()
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val hdfsPath = new Path(path)
    if(fs.exists(hdfsPath)) fs.delete(hdfsPath,true)
  }

}
