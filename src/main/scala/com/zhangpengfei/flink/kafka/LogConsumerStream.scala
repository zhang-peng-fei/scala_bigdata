package com.zhangpengfei.flink.kafka

import java.util.Properties

import com.zhangpengfei.util.CommonPro
import org.apache.commons.lang.StringUtils
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
  * 消费api log 到hdfs
  * 流模式，oldplanner环境变量设置
  */
object LogConsumerStream {

  def main(args: Array[String]): Unit = {

    val fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val fsTableEnv = StreamTableEnvironment.create(env, fsSettings)

    // 初始化系统环境变量
    val sysProp: Properties = initSystem
    val hdfsPath = sysProp.getProperty("default.hdfs.path")
    // 初始化 kafka consumer source
    val myConsumer: FlinkKafkaConsumer011[ApiCallLog] = initConsumerSource(sysProp)
    // 初始化 hdfs sink
    val hdfsSink: BucketingSink[ApiCallLog] = initHdfsSink(hdfsPath)

    // 算子逻辑
    val res1 = env.addSource(myConsumer)
    res1.map(x => x.toString.split("\t")).print()
//    res1.addSink(hdfsSink)

    env.execute(LogConsumerStream.getClass.toString)
  }

  /**
    * 初始化 kafka 的验证变量
    * @return
    */
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

  /**
    * 初始化 hdfs
    * @param hdfsPath
    * @return
    */
  private def initHdfsSink(hdfsPath: String): BucketingSink[ApiCallLog] = {
    val hdfsSink = new BucketingSink[ApiCallLog](hdfsPath)
    hdfsSink.setBucketer(new MothBucketer[ApiCallLog])
    hdfsSink.setBatchSize(CommonPro.loadProperties.getProperty("bucket.batch.size").toLong) // this is 400 MB,
    hdfsSink.setBatchRolloverInterval(CommonPro.loadProperties.getProperty("bucket.batch.rollover.interval").toLong); // this is 20 mins
    hdfsSink.setUseTruncate(false) //Could not delete truncate test file. You can disable support for truncate() completely via BucketingSink.setUseTruncate(false).
    hdfsSink
  }

  /**
    * 初始化 kafka 消费者的配置
    * @param sysProp
    * @return
    */
  private def initConsumerSource(sysProp: Properties): FlinkKafkaConsumer011[ApiCallLog] = {
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
    //    val myConsumer = new FlinkKafkaConsumer011("log_dls", new JSONKeyValueDeserializationSchema(true), properties)
    val myConsumer = new FlinkKafkaConsumer011("log_dls", new ApiCallLogKafkaDeserializationSchema(), properties)
    //    测试环境指定分区和偏移量
    if ("dev".equals(CommonPro.loadSysProperties.getProperty("profiles.active"))) {
      val specificStartOffsets = new java.util.HashMap[KafkaTopicPartition, java.lang.Long]()
      specificStartOffsets.put(new KafkaTopicPartition("log_dls", 0), 0L)
      myConsumer.setStartFromSpecificOffsets(specificStartOffsets)
    } else {
      myConsumer.setStartFromEarliest()
    }
    myConsumer
  }
}
