package com.zhangpengfei.spark_demo.demo.structredstreaming

import com.zhangpengfei.util.CommUtils
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}

object wordCount1 {

  implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
  private val stringEncoder: Encoder[String] = Encoders.STRING

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("structredstreamingcount")
      .master("local[*]")
      .getOrCreate()

    val lines = sparkSession.readStream
      .text(CommUtils.getBasicPath + "fileDir/wc")
    val wc = lines.as(Encoders.STRING)
      .flatMap(_.split(" "))(Encoders.STRING)
      .groupBy("value")
      .count()
      .orderBy("count")



    val query = wc.writeStream
      .format("console")
      .outputMode("complete")
      .start()
    query.awaitTermination()

    wc.show()


//    wc1(sparkSession)
//    wc2(sparkSession) // 输出模式为complete模式


  }
  private def wc2(sparkSession: SparkSession) = {
    val dataFrame = sparkSession.readStream
      .format("socket")
      .option("local", "localhost")
      .option("port", 9999)
      .load()
    val word = dataFrame.as[String](Encoders.STRING).flatMap(_.split(" "))(Encoders.STRING)
    val count = word.groupBy("value")
      .count()

    val query = count.writeStream
      .outputMode("complete")
      .format("console")
      .start()
    query.awaitTermination()

  }

  private def wc1(sparkSession: SparkSession):DataFrame = {
    val dataFrame = sparkSession.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load()
    val word = dataFrame.as[String](Encoders.STRING).flatMap(_.split(" "))(Encoders.STRING)
    val count = word.groupBy("value").count()
    return  count
  }



}
