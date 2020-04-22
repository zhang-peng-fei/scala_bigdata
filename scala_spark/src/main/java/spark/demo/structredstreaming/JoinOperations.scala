package spark.demo.structredstreaming

import com.zhangpengfei.util.CommUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object JoinOperations {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("joinOperation")
      .master("local[*]")
      .getOrCreate()

    val path = CommUtils.getBasicPath + "fileDir/csv"
    println("路径：" + path)
    // 流1
    val schoolData = sparkSession.readStream
      .schema(new StructType()
        .add("name", "string")
        .add("school", "string"))
      .option("seq", ",")
      .csv(path + "/school")

    // 流2
    val userData = sparkSession.readStream
      .schema(new StructType()
        .add("name", "string")
        .add("gender", "string")
        .add("age", "integer")
        .add("time", "string"))
      .option("seq", ",")
      .csv(path + "/user")

    // join 开始
//    val resData = userData.join(schoolData, Seq("name"), "full")
    //      .where("name = zhangsan")


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


    println("userData:")
    userData.foreach(print(_))
    println()
    println("schoolData:")
    schoolData.foreach(print(_))
    println()
    println("joinRes:")
    //    resData.foreach(print(_))

  }
}
