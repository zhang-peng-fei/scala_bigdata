package spark.demo.sparkstreaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object SparkStreamKafkaSourceDemo1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(SparkStreamKafkaSinkDemo1.getClass.getName)
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


    val context = new SparkContext(conf)
    val streamingContext = new StreamingContext(context, Seconds(10))


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.78.135:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "log_dls_consumer",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("log_dls")

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream.map(record => (record.key, record.value))
      .print()

    // Import dependencies and create kafka params as in Create Direct Stream above

    val offsetRanges = Array(
      // topic, partition, inclusive starting offset, exclusive ending offset
      OffsetRange("test", 0, 0, 100),
      OffsetRange("test", 1, 0, 100)
    )
    //    KafkaUtils.createRDD[Object, Object](streamingContext, kafkaParams, offsetRanges, PreferConsistent)
    //    KafkaUtils.createRDD[String, String](context, kafkaParams, offsetRanges, PreferConsistent)

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
    }

    // 开始计算
    streamingContext.start()
    // 等待计算结束
    streamingContext.awaitTermination()
  }
}
