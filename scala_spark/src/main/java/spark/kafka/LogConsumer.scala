package spark.kafka

import org.apache.spark.sql.SparkSession

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
      .option("kafka.bootstrap.servers", "192.168.78.135:9092")
      .option("subscribe", "log_dls")
      .load()

    ds1.foreach(println(_))
    /*ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)](Encoders.tuple(Encoders.STRING, Encoders.STRING))
*/
    ds1.writeStream
      .format("console")
      .outputMode("update")
      .start()


  }

}
