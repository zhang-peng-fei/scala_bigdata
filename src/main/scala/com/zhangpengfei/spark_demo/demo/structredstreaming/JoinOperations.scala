package com.zhangpengfei.spark_demo.demo.structredstreaming

import com.zhangpengfei.util.CommUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object JoinOperations {
  def main(args: Array[String]): Unit = {

    val school = new StructType()
      .add("name", "string")
      .add("school", "string")

    val sparkSession = SparkSession.builder()
      .appName("joinOperation")
      .master("local[*]")
      .getOrCreate()

    val path = CommUtils.getBasicPath + "fileDir/csv/school"
    print("路径："+path)
    // 流1
    val schoolData = sparkSession.readStream
      .schema(school)
      .option("seq", ",")
      .csv(path)
    // 流2
    val userData = sparkSession.readStream
      .schema(school)
      .option("seq", ",")
      .csv(path)

    // start 流1，流2
    val schoolQuery = schoolData.writeStream
      .format("console")
      .outputMode("update")
      .start()
    schoolQuery.awaitTermination()
    val userQuery = userData.writeStream
      .format("console")
      .outputMode("update")
      .start()
    userQuery.awaitTermination()

    // join 开始
    val resData = userData.join(schoolData,Seq("name", "age")).where("name = zhangsan")
    resData.foreach(print(_))
  }
}
