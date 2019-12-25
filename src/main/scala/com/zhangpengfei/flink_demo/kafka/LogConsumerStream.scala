package com.zhangpengfei.flink_demo.kafka

import java.util.Properties

import com.google.gson.JsonObject
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

import scala.util.parsing.json.JSONObject

object LogConsumerStream {

  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.78.135:9092")
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "192.168.78.135:2181")
    properties.setProperty("group.id", "op_log_consumer")

    val myConsumer = new FlinkKafkaConsumer010[String]("topic1", new SimpleStringSchema(), properties)

//    myConsumer.setStartFromEarliest()
    myConsumer.setStartFromTimestamp(System.currentTimeMillis())
    //    myConsumer.setStartFromLatest() // start from the latest record


    env.addSource(myConsumer)
      .print()

    env.execute()
  }
}
