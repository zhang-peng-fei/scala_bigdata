package spark.transformation

import com.zhangpengfei.util.CommUtils
import org.apache.spark.{SparkConf, SparkContext}

object Demo1 {

  def main(args: Array[String]): Unit = {
    /**
      * 使用算子前准备
      */
    // 新建 Spark 配置类
    val conf = new SparkConf().setAppName("算子demo").setMaster("local[*]")

    // 新建 SparkContext 上下文
    val sc = new SparkContext(conf)

    // 新建一个 Int 类型的数组
    val data = Array(1, 2, 3, 4, 5)

    // 使用 sc 加载数组
    val distData = sc.parallelize(data)
    distData.collect().foreach(print(_))


    /**
      *  spark 算子开发开始
      */

    // 加载外部文件
    val distFile = sc.textFile(CommUtils.getBasicPath + "fileDir/data.txt")
    val lineLengths = distFile.map(s => s.length) // 返回读取数据单个元素的长度
    val totalLength = lineLengths.reduce((a, b) => a + b) // 求总长
    val persistData = lineLengths.persist() // 持久化 lineLengths，用于以后重复使用
    println("文件字符总长："+totalLength)
    println("持久化的数据："+persistData.collect().map(print(_)))


  }
}
