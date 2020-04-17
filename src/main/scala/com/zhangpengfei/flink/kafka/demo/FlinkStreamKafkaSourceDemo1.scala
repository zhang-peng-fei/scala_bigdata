package com.zhangpengfei.flink.kafka.demo

import org.apache.flink.api.scala._

import java.util.Properties

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object FlinkStreamKafkaSourceDemo1 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.78.135:9092")
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "192.168.78.135:2181")
    properties.setProperty("group.id", "log_dls_consumer")
    val stream = env
      .addSource(new FlinkKafkaConsumer010[String]("log_dls", new SimpleStringSchema(), properties))
      .print()

    env.execute(FlinkStreamKafkaSourceDemo1.getClass.getName)
  }
}
