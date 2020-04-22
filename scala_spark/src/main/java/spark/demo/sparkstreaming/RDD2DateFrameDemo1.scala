package spark.demo.sparkstreaming

import com.zhangpengfei.util.CommUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object RDD2DateFrameDemo1 {

  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)


    val conf = new SparkConf()
      .set("spark.sql.warehouse.dir", "file:///e:/tmp/spark-warehouse")
      .set("spark.some.config.option", "some-value")

    val ss = SparkSession
      .builder()
      .config(conf)
      .appName("RDDToDF")
      .master("local[2]")
      .getOrCreate()
    /**
     * 方法一
     * 使用反射机制推理出schema
     */
    val path = CommUtils.filePath+"fileDir\\people.txt"

    import ss.implicits._
    val sc = ss.sparkContext

    val RDDPerson1 = sc.textFile(path)
    val RDDPerson = RDDPerson1.map(_.split(", ")).map(p => Person(p(0), p(1).trim.toInt))

    val DFPerson = RDDPerson.toDF()

    DFPerson.printSchema()

    DFPerson.select($"name", $"age").show()

    /**
     * 方法二
     * 自定义schema
     */

    import ss.implicits._

    val schemaString = "name, age"   //1、创建schema所需要的字段名的字符串，用特殊符号连接，如“,”

    //2、遍历schema的fileName，将每个fileName封装成StructField对象，实质是定义每一个file的数据类型、属性。 Array[StructField]
    val fields = schemaString.split(", ").map(fileName => StructField(fileName, StringType, nullable = true))
    //3、将 Array[StructField]转化成schema
    val schema = StructType(fields)

    val peopleRDD = sc.textFile(path)
    //4、构建RDD[Row]
    val rowRDD = peopleRDD.map(_.split(", ")).map(att=>Row(att(0),att(1).trim))

    val peopleDF = ss.createDataFrame(rowRDD, schema)

    peopleDF.select($"name",$"age").show()
  }
}
