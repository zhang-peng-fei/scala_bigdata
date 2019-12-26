package com.zhangpengfei.flink.kafka

import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema

object LogConsumerStream {

  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "10.142.117.55:9093,10.142.117.56:9093,10.142.117.57:9093")
    properties.setProperty("zookeeper.connect", "10.142.114.211:2181,10.142.114.231:2181,10.142.114.241:2181")
//    properties.setProperty("bootstrap.servers", "192.168.78.135:9092")
//    properties.setProperty("zookeeper.connect", "192.168.78.135:2181")
    properties.setProperty("group.id", "op_log_consumer")

    val myConsumer = new FlinkKafkaConsumer010("log_dls", new JSONKeyValueDeserializationSchema(true), properties)

    val specificStartOffsets = new java.util.HashMap[KafkaTopicPartition, java.lang.Long]()
    specificStartOffsets.put(new KafkaTopicPartition("log_dls", 0), 26L)
    myConsumer.setStartFromSpecificOffsets(specificStartOffsets)

    val hdfsSink = new BucketingSink[JsonNode]("hdfs://10.142.149.245:8082/user/hive/warehouse/")
    hdfsSink.setBucketer(new MothBucketer[JsonNode])
    //    hdfsSink.setWriter(new SequenceFileWriter[String])
    hdfsSink.setBatchSize(1024 * 1024 * 400) // this is 400 MB,
    hdfsSink.setBatchRolloverInterval(20 * 60 * 1000); // this is 20 mins

    env.addSource(myConsumer)
      .map(r => r.get("value"))
      //        .flatMap(res=>res)
      //        .map(node=>node.isObject)
      //      .filter(node => null != node.get("value") && node.get("value").isContainerNode)
      .addSink(hdfsSink)


    env.execute()
  }
}
