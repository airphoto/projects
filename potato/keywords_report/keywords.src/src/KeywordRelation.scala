import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.JavaConversions._
/**
 * Created by hadoop on 2016/5/20.
 */
object KeywordRelation {
  def main(args: Array[String]) {

    if(args.length<3){
      println("KeywordRelation <keywordSetPath> <day> <outPathPre>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("KeywordRelation")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val keywordSetPath = args(0)
    val day = args(1)
    val outPath = args(2)+"/"+day

    sqlContext.sql("use formatlog")

    //关键词词包，并将词包数据转化成Set
    val keywordSet = sc.textFile(keywordSetPath).map(x=>(x,None)).collectAsMap().keySet

    //bidder的keywords
    val data = sqlContext.table("bid_request").where("d="+day).select("keywords")

    //keywords中的在keywordSet中出现的每个元素相互配对
    val exitWords = data.flatMap(x=>{
      val ks = x.getList[String](x.fieldIndex("keywords"))
      val sortedList = ks
      val mapSets = for(i<-0 until (sortedList.size-1);
        j<-(i+1) until sortedList.size) yield {
        if(sortedList(i)!="" && sortedList(j)!="" && sortedList(i)!=sortedList(j)  && keywordSet.contains(sortedList(i)) && keywordSet.contains(sortedList(j)))
          (sortedList(i)+"->"+sortedList(j),1)
        else ("",1)
      }
      mapSets
    }).filter(x=>(x._1!="" && x._1.split("->").length==2))

    //将相同的数据相加
    val reduceData = exitWords.reduceByKey(_+_)

    //配对元素之间相互作为key
    val k2kData = reduceData.flatMap(x=>{
      val fields = x._1.split("->")
      List((fields(0),fields(1)+"->"+x._2),(fields(1),fields(0)+"->"+x._2))
    })

    //将key相同的数据连接
    val relationData = k2kData.reduceByKey(_+"|"+_)

    //转化成json
    val jsonData = relationData.map(x=>{
      val k = x._1
      val values = x._2.split("\\|")
      var sum = 0
      val relations = for(i<-values) yield {
        val fields = i.split("->")
        val name = fields(0)
        val count = fields(1).toInt
        sum += count
        "\""+name+"\":"+count
      }
      "\""+k+"\":{\"relation\":{"+relations.mkString(",")+"},\"total\":"+sum+"}"
    })

    //路径存在就删除
    existsPath(outPath)

    //存储数据
    jsonData.coalesce(20).saveAsTextFile(outPath)

    sc.stop()
  }

  def existsPath(path:String): Unit ={
    val conf = new Configuration()
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val hdfsPath = new Path(path)
    if(fs.exists(hdfsPath)) fs.delete(hdfsPath,true)
  }
}
