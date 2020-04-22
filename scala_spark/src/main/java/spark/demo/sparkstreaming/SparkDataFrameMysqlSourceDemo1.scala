package spark.demo.sparkstreaming

import java.util.Properties

import org.apache.spark.sql.SparkSession

object SparkDataFrameMysqlSourceDemo1 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(SparkDataFrameMysqlSourceDemo1.getClass.getName)
      .master("local[2]")
      .getOrCreate()
    /**
     * 方法一
     * spark.read.jdbc
     */
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")

    val url = "jdbc:mysql://localhost:3306/report_db?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC"

    val dataFrame = spark.read.jdbc(url,"report_tenant",prop).select("id","tenantName","hadoopHdfsQuotaSize").where("id <> 3").show()
    print(dataFrame)


    /**
     * 方法二
     * spark.read.format.load
     */
    val dataDF = spark.read.format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/log?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC")
      .option("dbtable","inter_scene_config")
      .option("user","root")
      .option("password","root")
      .load()

    dataDF.createOrReplaceTempView("tmptable")

    val sql = "select * from tmptable where scene_id <> 13"

    spark.sql(sql).show()


    spark.stop()
  }
}
