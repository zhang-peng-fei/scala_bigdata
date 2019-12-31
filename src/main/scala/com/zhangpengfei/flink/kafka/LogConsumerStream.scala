package com.zhangpengfei.flink.kafka

import java.util
import java.util.Properties

import com.zhangpengfei.util.CommonPro
import org.apache.commons.lang.StringUtils
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.SequenceFileWriter
import org.apache.flink.streaming.connectors.fs.bucketing.{Bucketer, BucketingSink}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.hadoop.io.Text

object LogConsumerStream {

  def main(args: Array[String]): Unit = {
    val fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val fsTableEnv = StreamTableEnvironment.create(env, fsSettings)

    // 初始化系统环境变量
    val sysProp: Properties = initSystem
    val hdfsPath = sysProp.getProperty("default.hdfs.path")
    // 初始化 kafka consumer source
    val myConsumer: FlinkKafkaConsumer010[ObjectNode] = initConsumerSource(sysProp)
    // 初始化 hdfs sink
    val hdfsSink: BucketingSink[(String, String)] = initHdfsSink(hdfsPath)

    // 算子逻辑
    env.addSource(myConsumer)
      .map(r => new ObjectMapper().readTree(r.get("value").toString().replace("\\n", "")))
      .map(x => x.elements())
      .map(x => {
        var res = ""
        var month_id = ""
        var day_id = ""
        var y = 0
        var z = 0
        while (x.hasNext) {
          val str = x.next().asText()
          res += str + "|"
          y += 1
          z += 1
          if (y == 17)
            month_id = str
          if (z == 8)
            day_id = str
        }
        (month_id + "-" + day_id, res.substring(0, res.length - 3))
//        res.substring(0, res.length - 3)
      })
      .addSink(hdfsSink)


    env.execute()
  }

  private def initSystem: Properties = {
    val sysProp = CommonPro.loadProperties
    if (!StringUtils.isBlank(sysProp.getProperty("java.security.auth.login.config"))) {
      System.setProperty("java.security.auth.login.config", sysProp.getProperty("java.security.auth.login.config"))
    }
    if (!StringUtils.isBlank(sysProp.getProperty("java.security.krb5.conf"))) {
      System.setProperty("java.security.krb5.conf", sysProp.getProperty("java.security.krb5.conf"))
    }
    if (!StringUtils.isBlank(sysProp.getProperty("sun.security.krb5.debug"))) {
      System.setProperty("sun.security.krb5.debug", sysProp.getProperty("sun.security.krb5.debug"))
    }
    if (!StringUtils.isBlank(sysProp.getProperty("javax.security.auth.useSubjectCredsOnly"))) {
      System.setProperty("javax.security.auth.useSubjectCredsOnly", sysProp.getProperty("javax.security.auth.useSubjectCredsOnly"))
    }
    sysProp
  }

  private def initHdfsSink(hdfsPath: String): BucketingSink[(String, String)] = {
    val hdfsSink = new BucketingSink[(String, String)](hdfsPath)
    hdfsSink.setBucketer(new MothBucketer[(String, String)])
//    hdfsSink.setWriter(new SequenceFileWriter[Text, Text])
    hdfsSink.setBatchSize(1024) // this is 400 MB,
    hdfsSink.setBatchRolloverInterval(1000); // this is 20 mins
    hdfsSink.setUseTruncate(false) //Could not delete truncate test file. You can disable support for truncate() completely via BucketingSink.setUseTruncate(false).
    hdfsSink
  }

  private def initConsumerSource(sysProp: Properties): FlinkKafkaConsumer010[ObjectNode] = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", sysProp.getProperty("bootstrap.servers"))
    properties.setProperty("group.id", sysProp.getProperty("group.id"))
    properties.setProperty("enable.auto.commit", "false")
    properties.setProperty("session.timeout.ms", "30000")
    //每次poll最多获取10条数据
    properties.setProperty("max.poll.records", "10")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    if (!StringUtils.isBlank(sysProp.getProperty("sasl.mechanism"))) {
      properties.setProperty("sasl.mechanism", sysProp.getProperty("sasl.mechanism"))
    }
    if (!StringUtils.isBlank(sysProp.getProperty("security.protocol"))) {
      properties.setProperty("security.protocol", sysProp.getProperty("security.protocol"))
    }
    if (!StringUtils.isBlank(sysProp.getProperty("sasl.jaas.config"))) {
      properties.setProperty("sasl.jaas.config", sysProp.getProperty("sasl.jaas.config"))
    }
    if (!StringUtils.isBlank(sysProp.getProperty("sasl.kerberos.service.name"))) {
      properties.setProperty("sasl.kerberos.service.name", sysProp.getProperty("sasl.kerberos.service.name"))
    }
    val myConsumer = new FlinkKafkaConsumer010("log_dls", new JSONKeyValueDeserializationSchema(true), properties)
    val specificStartOffsets = new java.util.HashMap[KafkaTopicPartition, java.lang.Long]()
    specificStartOffsets.put(new KafkaTopicPartition("log_dls", 0), 29L)
    myConsumer.setStartFromSpecificOffsets(specificStartOffsets)

    //    myConsumer.setStartFromEarliest()
    myConsumer
  }
}
