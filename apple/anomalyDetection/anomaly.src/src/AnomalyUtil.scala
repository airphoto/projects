import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

/**
 * Created by hadoop on 2016/3/3.
 */
object AnomalyUtil {
  def DateFormat(time:Int):String={
    var sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    var date:String = sdf.format(new Date((time.toLong*1000)))
    date
  }

  def existsPath(path:String):Unit={
    val config = new Configuration()
    val fs = FileSystem.get(config)
    val hdfsPath = new Path(path)
    if (fs.exists(hdfsPath)) fs.delete(hdfsPath,true)
  }
}
