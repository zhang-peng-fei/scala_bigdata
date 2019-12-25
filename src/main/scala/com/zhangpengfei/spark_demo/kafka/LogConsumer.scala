package com.zhangpengfei.spark_demo.kafka

import org.apache.spark.sql.{Encoders, SparkSession}

object LogConsumer {

  def main(args: Array[String]): Unit = {

    // 初始化 SparkSession
    val spark = SparkSession.builder()
      .appName("kafkaStreaming")
      .master("local[*]")
      .getOrCreate()



    // 订阅1个 topic
    val ds1 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1")

      .load()
    ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)](Encoders.tuple(Encoders.STRING,Encoders.STRING))

  }

}
