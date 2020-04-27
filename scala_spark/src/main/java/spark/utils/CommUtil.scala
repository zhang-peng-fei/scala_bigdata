package spark.utils

import com.zhangpengfei.util.CommUtils

object CommUtil {
  val dataPath = "data/"


  def main(args: Array[String]): Unit = {
    val path = this.getClass.getClassLoader.getResource("").getPath
    System.out.println(path)
    System.out.println(dataPath)
    System.out.println(CommUtils.getBasicPath)

  }

  def getResourcePath = this.getClass.getClassLoader.getResource("").getPath

}
