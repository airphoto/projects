import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by hadoop on 2016/5/17.
 */
object KeywordsReportHour{
  def main(args: Array[String]) {
    if(args.size<3){
      println("usage : KeywordsReport <day> <hour> <pathPre>")
      System.exit(1)
    }
    @transient
    val sparkConf = new SparkConf()
    @transient
    val sc  =new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val day = args(0)//"20160422"
    val hour = args(1).toInt//10
    val pathPre = args(2)//"/test/work/keywords"

    sqlContext.sql("use formatlog")

    val data = sqlContext.table("deliveries").where("request_time!=0").where("d="+day).where("h="+hour)

    if(data.count()==0){
      println("empty data")
      System.exit(1)
    }

    //获取本时间段内所有的adid
    val adidThisHour = data.select("hqadid").map(x=>x.getString(x.fieldIndex("hqadid"))).distinct.collect

    //从前端接口取得adid对应的词包文件路径
    val str = GetKeywordsFilePath.getJson

    //将获取的json转化成dataFrame
    val df = sqlContext.read.json(sc.parallelize(List(str)))


    val adid = df.schema.fieldNames
    val values = df.map(x=>x.mkString(",")).collect()
    val keywordsPath = values(0).split(",").map(x=>"/user/work/keywords/keywords/"+x)
    //将adid和词包路径相匹配
    val maps = adid.zip(keywordsPath)

    //获取当前时间内adid对应的路径
    val paths = for((k,v)<-maps) yield{
      if(adidThisHour.contains(k)) v
      else ""
    }
    //获取词包
    val keywordsRDD = sc.textFile(paths.filter(_!="").mkString(","))

    @transient
    val keywordSet = keywordsRDD.map(x=>(x,None)).collectAsMap().keySet

    //第一个匹配到的关键词对应的impression，click，cost，bid
    val keyData = data.map(x=>{
      val keywords = x.getList[String](x.fieldIndex("keywords"))
      val impression = x.getInt(x.fieldIndex("impression"))
      val click = x.getInt(x.fieldIndex("click"))
      val bid = x.getInt(x.fieldIndex("bidprice"))
      val cost = x.getString(x.fieldIndex("price")).toDouble
      val keyword = matchFirst(keywords,keywordSet)
      val adid = x.getString(x.fieldIndex("hqadid"))
      val platform = x.getString(x.fieldIndex("hqsource"))
      (keyword+"\t"+adid+"\t"+platform,(impression,click,cost,bid.toDouble))
    })

    //根据关键词统计  keyword  impression  click  cost  bid
    val result = keyData.filter(_._1.split("\t").size==3).reduceByKey((x,y)=>{
      (x._1+y._1,x._2+y._2,x._3+y._3,x._4+y._4)
    }).map(x=>{
      val fields = x._1.split("\t")
      val key = fields(0)
      val adid = fields(1)
      val platform = fields(2)
      key+"\t"+x._2._1+"\t"+x._2._2+"\t"+x._2._3.toInt+"\t"+x._2._4.toInt+"\t"+adid+"\t"+platform
    })

    val outPath = pathPre+"/report/"+day+"/"+hour
    Utils.existsPath(outPath)

    //存储数据
    result.coalesce(3).saveAsTextFile(outPath)

    sqlContext.sql("alter table hq_keywords_report_hour drop partition(d="+day+",h="+hour+")")
    sqlContext.sql("alter table hq_keywords_report_hour add partition(d="+day+",h="+hour+") location '"+outPath+"'")

    sc.stop()
  }


  /**
   * 获取最新的关键词的文件的路径
   * @param path
   * @return
   */
  def getKeywordsFile(path:String)={
    val conf = new Configuration()
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val hdfsPath = new Path(path)
    val files = fs.listStatus(hdfsPath)
    val maps = for(file<-files) yield {
      val path = file.getPath
      val dir = path.getName
      (dir.toLong->path)
    }

    maps.sortBy(_._1).reverse.toList(0)._2.toString
  }

  /**
   * 在当前的keywords中返回第一个匹配成功的关键词，否则就返回"unknown"
   * @param keywords
   * @param keywordSet
   * @return
   */
  def matchFirst(keywords:java.util.List[String],keywordSet:scala.collection.Set[String]):String={
    for (i <- 0 until keywords.size){
      if (keywordSet.contains(keywords.get(i))) {
        return keywords.get(i)
      }
    }
    "unknown"
  }
}
