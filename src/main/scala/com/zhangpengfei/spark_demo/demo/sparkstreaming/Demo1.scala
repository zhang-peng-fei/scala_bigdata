package com.zhangpengfei.spark_demo.demo.sparkstreaming

import com.zhangpengfei.util.CommUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Demo1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("demo1")
//    val context = new StreamingContext(conf, Seconds(1))
    val context = new SparkContext(conf)
    val path = CommUtils.getBasicPath
    val textFile = context.textFile(path + "/fileDir/wc/wc.txt")
    textFile.map(x => x.split(" "))
  }
}
