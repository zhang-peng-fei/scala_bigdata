package com.zhangpengfei.spark_demo.demo.structredstreaming

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object windowoperation {



  def main(args: Array[String]): Unit = {
    // 定义数据格式
    val wordSchema = new StructType()
      .add("timestamp", "timestamp")
      .add("word", "string")

    // 构建 SparkSession
    val sparkSession = SparkSession.builder().appName("windowoperation")
      .getOrCreate()

    // 读取数据为 DataFrame
    val df = sparkSession.readStream
      .option("seq", ";")
      .schema(wordSchema)
      .csv("/opt/zd")

//    df.as[]
    // 将数据按窗口和单词分组，并计算每个组的计数
    df.groupBy()


  }
}
