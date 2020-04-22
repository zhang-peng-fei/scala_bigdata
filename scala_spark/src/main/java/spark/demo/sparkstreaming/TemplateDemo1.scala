package spark.demo.sparkstreaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TemplateDemo1 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName(SparkStreamKafkaSinkDemo1.getClass.getName)
      .setMaster("local[1]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkContext = new SparkContext(sparkConf)
    val sparkStreamingContext = new StreamingContext(sparkContext, Seconds(10))

    //--------------------operate start--------------------

    //--------------------operate end----------------------

    // 开始接收数据并使用 streamingContext.start()进行处理。
    sparkStreamingContext.start()
    // 使用 streamingContext.awaitTermination()等待处理停止(手动或由于任何错误)。
    sparkStreamingContext.awaitTermination()
  }
}
