package com.zhangpengfei.flink.kafka

import java.time.ZoneId

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.fs.StringWriter
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.descriptors.{Json, Kafka, Schema}
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.types.Row

object LogConsumerTable {

  def main(args: Array[String]): Unit = {

    val zookeeper = "192.168.78.135:2181"
    val kafka = "192.168.78.135:9092"
    val hdfsPath = "hdfs://192.168.78.135:9000/user/hive/bendi/"
    //    val zookeeper = "10.142.114.211:2181,10.142.114.231:2181,10.142.114.241:2181"
    //    val kafka = "10.142.117.55:9093,10.142.117.56:9093,10.142.117.57:9093"
    //    val hdfsPath = "hdfs://10.142.149.245:8082/user/hive/warehouse/api_log/"

    val fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.enableCheckpointing(1000)
    val fsTableEnv = StreamTableEnvironment.create(env, fsSettings)
    // 注册数据源，{"username":"zhangsan","age":23,"gender":"nan"}
    fsTableEnv
      .connect(
        new Kafka()
          .version("0.10")
          .topic("log_dls")
          //          .startFromEarliest()
          //          .startFromLatest()
          .startFromSpecificOffset(0, 30L)
          .property("zookeeper.connect", zookeeper)

          .property("bootstrap.servers", kafka))
      .withFormat(
        new Json()
          .failOnMissingField(false)
          .deriveSchema()
      )
      .withSchema(
        new Schema()
          .field("apiType", "VARCHAR")
          .field("backendResponseCode", "VARCHAR")
          .field("businessResponseCode", "VARCHAR")
          .field("callByte", "INT")
          .field("callEndTime", "VARCHAR")
          .field("callIp", "VARCHAR")
          .field("callStartTime", "VARCHAR")
          .field("dayId", "INT")
          .field("errLevel", "VARCHAR")
          .field("gatewayBusinessResponseCode", "VARCHAR")
          .field("gatewayResponseCode", "VARCHAR")
          .field("host", "VARCHAR")
          .field("hourId", "INT")
          .field("logCnt", "VARCHAR")
          .field("logId", "INT")
          .field("method", "VARCHAR")
          .field("monthId", "INT")
          .field("rawData", "VARCHAR")
          .field("reqCnt", "VARCHAR")
          .field("requestForwardTime", "VARCHAR")
          .field("requestParam", "VARCHAR")
          .field("requestReceivedTime", "VARCHAR")
          .field("requestSize", "VARCHAR")
          .field("responseForwardTime", "VARCHAR")
          .field("responseParam", "VARCHAR")
          .field("responseReceivedTime", "VARCHAR")
          .field("responseSize", "VARCHAR")
          .field("resultFlag", "INT")
          .field("sId", "VARCHAR")
          .field("seqId", "VARCHAR")
          .field("subTime", "INT")
          .field("traceId", "VARCHAR")
          .field("uri", "VARCHAR")
          .field("userAgent", "VARCHAR")
          .field("userId", "VARCHAR")
      )
      .inAppendMode()
      .registerTableSource("api_call_log")
    val stream: Table = fsTableEnv.scan("api_call_log")


    // 注册数据结果表（本地HDFS文件，两种方式[BucketingSink,StreamingFileSink]）
    val hdfsStream: DataStream[Row] = fsTableEnv.toAppendStream(stream)

    val hdfsSink1 = new BucketingSink[Row](hdfsPath)
    hdfsSink1.setBucketer(new DateTimeBucketer("yyyy-MM-dd--HHmm", ZoneId.of("America/Los_Angeles")))
    hdfsSink1.setWriter(new StringWriter[Row])
    hdfsSink1.setBatchSize(10) //
    hdfsSink1.setBatchRolloverInterval(20); //
    hdfsStream.addSink(hdfsSink1)


    // 数据处理，读和写(打印到控制台)
    fsTableEnv
      .toAppendStream[Row](stream)
      .print()
    //      .setParallelism(3)
    // 注册数据结果表（本地文件）
    val sink: CsvTableSink = new CsvTableSink(
      "C:/Users/张朋飞/Desktop/c.txt", // output path
      "|", // optional: delimit files by '|'
      1, // optional: write to a single file
      WriteMode.OVERWRITE) // optional: override existing files

    /*fsTableEnv.registerTableSink(
      "res1",
      Array[String]("apiType", "backendResponseCode", "businessResponseCode", "callByte", "callEndTime", "callIp", "callStartTime"
        , "dayId", "errLevel", "gatewayBusinessResponseCode", "gatewayResponseCode", "host", "hourId", "logCnt", "logId", "method"
        , "monthId", "rawData", "reqCnt", "requestForwardTime", "requestParam", "requestReceivedTime", "requestSize", "responseForwardTime", "responseParam", "responseReceivedTime"
        , "responseSize", "resultFlag", "sId", "seqId", "subTime", "traceId", "uri", "userAgent", "userId"),
      Array[TypeInformation[_]](Types.STRING, Types.STRING, Types.STRING, Types.INT, Types.STRING, Types.STRING, Types.STRING, Types.INT
        , Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.INT, Types.STRING, Types.INT, Types.STRING, Types.INT, Types.STRING
        , Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING
        , Types.INT, Types.STRING, Types.STRING, Types.INT, Types.STRING, Types.STRING, Types.STRING, Types.STRING),
      sink)*/
    //    stream.insertInto("res1")

    fsTableEnv.execute("wer")
  }
}
