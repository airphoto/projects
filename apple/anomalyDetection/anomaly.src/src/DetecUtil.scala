import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

/**
 * Created by hadoop on 2016/3/7.
 */
object DetecUtil {
  def getResult(x:Double,sigma_2:Double,miu:Double)={
    val f = -(getDiff_2(x,miu)/(2*sigma_2))
    (1/(Math.pow(Math.PI*2,0.5)*Math.pow(sigma_2,0.5)))*Math.pow(Math.E,f)
  }

  def getDiff_2(x:Double,miu:Double)={
    Math.pow((x-miu),2)
  }

  //根据二分法查找到tmp对应的分段的index
  def getIndex(tmp:Double,arr:Array[Double],low:Int,high:Int):Int ={
    if(tmp==0.0) 1
    else if (low>high) low
    else {
      val mid = (low + high) / 2
      if (tmp > arr(mid)) {
        getIndex(tmp, arr, mid + 1, high)
      } else if(tmp < arr(mid)){
        getIndex(tmp, arr, low, mid - 1)
      }else mid
    }
  }

  def pathExists(path:String):Unit={
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val hdfsPath = new Path(path)
    if(fs.exists(hdfsPath)) fs.delete(hdfsPath,true)
  }
}
