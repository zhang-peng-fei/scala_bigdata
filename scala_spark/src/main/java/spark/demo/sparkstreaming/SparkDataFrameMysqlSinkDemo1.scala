package spark.demo.sparkstreaming

import java.util.Properties

import com.zhangpengfei.util.CommUtils
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset, Row, SaveMode, SparkSession}

object SparkDataFrameMysqlSinkDemo1 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(SparkDataFrameMysqlSinkDemo1.getClass.getName)
      .master("local[2]")
      .getOrCreate()
    /**
     * 方法一
     * spark.read.jdbc
     */
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")

    val url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC"

    val dataSet: Dataset[Row] = spark.read.jdbc(url, "test1", prop)
      .select("id", "name", "age")
      .where("id <> 3")
    // 先读出，再写入另外一个表
    dataSet.write.mode(SaveMode.Append).jdbc(url, "test1", prop)
    /**
     * 方法二
     * 通过构建DataFrame再写入
     * dataFrame.write.mode.jdbc
     */


    val rdd = spark.sparkContext.textFile(CommUtils.filePath+"fileDir\\people.json")

    val rdd2 = rdd.flatMap(_.split(", ")).distinct().zipWithIndex().map(t =>{Row(t._1,t._2)})

    val schema = StructType{
      List(
        StructField("name",StringType,true),
        StructField("age",LongType,true)
      )}

    val dataFrame:DataFrame = spark.createDataFrame(rdd2,schema)

    dataFrame.write.mode(SaveMode.Overwrite).jdbc(url,"test",prop)


    spark.stop()
  }
}
