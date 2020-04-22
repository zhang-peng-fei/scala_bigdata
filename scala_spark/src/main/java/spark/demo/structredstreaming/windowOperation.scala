package spark.demo.structredstreaming

import com.zhangpengfei.util.CommUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object windowOperation {

  def main(args: Array[String]): Unit = {
    // 定义数据格式
    val wordSchema = new StructType()
      .add("timestamp", "string")
      .add("word", "string")

    // 构建 SparkSession
    val sparkSession = SparkSession.builder()
      .appName("windowOperation")
      .master("local[*]")
      .getOrCreate()

    // 读取数据为 DataFrame
    val df = sparkSession.readStream
      .option("seq", ",")
      .schema(wordSchema)
      .csv(CommUtils.getBasicPath + "fileDir/csv")

    // 开始流
    val query = df.writeStream
      .format("console")
      .outputMode("update")
      .start()
    // 阻塞，直到查询停止或发生错误
    query.awaitTermination()


//    df.as[(Timestamp, String)](Encoders.kyro(new mutable.HashMap[Timestamp, String]()))
    // 将数据按窗口和单词分组，并计算每个组的计数
    val dataFrame = df.withWatermark("timestamp", "10 minutes")
      .groupBy()
      .count()
    dataFrame.foreach(print(_))


  }
}
