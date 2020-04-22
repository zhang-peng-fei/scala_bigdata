package spark.demo.structredstreaming

import org.apache.spark.sql.{Encoders, SparkSession}

object KafkaStreaming {
  def main(args: Array[String]): Unit = {
    // 初始化 SparkSession
    val sparkSession = SparkSession.builder()
      .appName("kafkaStreaming")
      .master("local[*]")
      .getOrCreate()
    // 读取 kafka 数据
    val dataFrame = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.78.135:9092")
      // 订阅一个 topic
      .option("subscribe", "log_dls")
      // 订阅多个 topic
//      .option("subscribe", "topic1,topic2")
      // 订阅多个 topic，通配符*
//      .option("subscribe", "topic*")

      // 显式指定 offset
//      .option("startingOffsets", """{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}""")
//      .option("endingOffsets", """{"topic1":{"0":50,"1":-1},"topic2":{"0":-1}}""")

      // 显示指定 offset，最早和最晚
//      .option("startingOffsets", "earliest")
//      .option("endingOffsets", "latest")
      .load()
//      .selectExpr("CAST(value AS STRING)")
//      .as[String](Encoders.STRING)
    // 类型转换
    dataFrame.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)](Encoders.tuple(Encoders.STRING,Encoders.STRING))

    // wordCount
//    val wordCounts = dataFrame.flatMap(_.split(" "))(Encoders.STRING).groupBy("value").count()

    val query = dataFrame.writeStream
      .format("console")
      .outputMode("update")
      .start()
    query.awaitTermination()

    dataFrame.foreach(println(_))
  }
}
