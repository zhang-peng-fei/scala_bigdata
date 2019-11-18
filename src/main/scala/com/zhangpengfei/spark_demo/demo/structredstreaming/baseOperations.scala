package com.zhangpengfei.spark_demo.demo.structredstreaming

import java.util.Date

import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.types.{DataType, StructType}

object baseOperations {


  def main(args: Array[String]): Unit = {
    case class DeviceData(device: String, deviceType: String, signal: Double, time: Date)


    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[DeviceData]

    // 构建 SparkSession
    val sparkSession = SparkSession.builder()
      .appName("basicOperation")
      .master("local[*]")
      .getOrCreate()

    // 定义数据 Schema
    val deviceSchema = new StructType()
      .add("device", "string")
      .add("deviceType", "string")
      .add("signal", "integer")
      .add("time", "string")

    // 将目录下的数据文件读取为 DataFrame，只要文件夹下多了新的文件，程序就会自动扫到，并执行下面的逻辑
    val df: DataFrame = sparkSession.readStream
      .option("seq", ",")
      .schema(deviceSchema)
      // 下面的目录必须是文件夹，不能是文件
      .csv("F:/bigdata/spark/resources/csv")

    // start 流程序，因为是流，所以输出模式为 update
    val query = df.writeStream
      .outputMode("update")
      .format("console")
      .start()
    query.awaitTermination()



    // 将 DataFrame 转换成 DataSet
    val ds: Dataset[DeviceData] = df.as[DeviceData](mapEncoder)

    // 无类型的 API，返回类型为 DataSet
    val value = df.select("device")
      .where("signal < 10")

    // 有类型的 API，返回类型为 DataSet
    val value1 = ds.filter(_.signal > 10)
      .map(_.device)(Encoders.STRING).foreach(print(_))


    df.groupBy("deviceType")
      .count()

    ds.groupByKey(_.deviceType)(Encoders.STRING)
      .agg(typed.avg(_.signal))

  }
}
