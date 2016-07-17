import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * Created by hadoop on 2016/1/15.
 *
 * args(0) 数据路径
 * args(1) 训练比例
 * args（2） 检验比例
 * args(3) 存储路径前缀
 */
object LR {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    // Load training data in LIBSVM format.

    val data = MLUtils.loadLibSVMFile(sc, args(0))

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(args(1).toDouble, args(2).toDouble), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(training)



    val conf = new org.apache.hadoop.conf.Configuration()
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val hdfsOutPath = new org.apache.hadoop.fs.Path(args(3))
    if (fs.exists(hdfsOutPath)) fs.delete(hdfsOutPath, true)//如果存储路径已经存在就删除

    // Save and load model
    model.save(sc, args(3)+"/model")

    model.clearThreshold

    val predictionAndLabels = test.map {
      case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
    }

    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    sc.makeRDD(Array("auPRC="+ metrics.areaUnderPR,"auROC="+metrics.areaUnderROC)).coalesce(1).saveAsTextFile(args(3)+"/auc")

    sc.stop()
  }
}
