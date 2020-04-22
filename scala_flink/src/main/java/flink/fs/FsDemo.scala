package flink.fs

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}

object FsDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment


    val a = Array("asdfasd", "12sdgdfg", "sadf", "132sdffffff")


    var hdfsPath = "hdfs://192.168.78.135:9000/user/hive/bendi/"

    val hdfsSink = new BucketingSink[Array[String]](hdfsPath)
    hdfsSink.setBucketer(new DateTimeBucketer[Array[String]]("yyyyMM"))
    //    hdfsSink.setWriter(new SequenceFileWriter[String])
    hdfsSink.setBatchSize(1024 * 1024 * 400) // this is 400 MB,
    hdfsSink.setBatchRolloverInterval(20 * 60 * 1000); // this is 20 mins

    val value = env.fromElements(a)

    value
//      .print()
      .addSink(hdfsSink)


    env.execute("FSDemo")
  }
}
