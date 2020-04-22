package flink.kafka

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

    val fsSettings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.enableCheckpointing(1000)
    val fsTableEnv = StreamTableEnvironment.create(env, fsSettings)
    // 注册数据源，{"username":"zhangsan","age":23,"gender":"nan"}
    fsTableEnv
      .connect(
        new Kafka()
          .version("0.10")
          .topic("log_dls")
          .startFromEarliest()
          //          .startFromLatest()
          .startFromSpecificOffset(0, 0L)
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
    stream
//      .filter("1")
      .select("*")


    fsTableEnv.execute("wer")
  }
}
