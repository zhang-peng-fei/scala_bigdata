package com.zhangpengfei.spark_demo.demo.structredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object createdataframeordataset {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("createdataframeordataset")
      .getOrCreate()

    val dataFrame = sparkSession.readStream
      .format("socket")
      .option("local", "localhost")
      .option("port", 9999)
      .load()

    val isStreaming = dataFrame.isStreaming

    dataFrame.printSchema()

    // 读取一个目录中自动写入的所有csv文件
    val userSchema = new StructType()
      .add("name", "string")
      .add("age", "integer")

    sparkSession.readStream
      .option("seq", "；")
      .schema(userSchema)
      .csv("/opt/zd")
  }
}
