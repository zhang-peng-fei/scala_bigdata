
package spark.demo.structredstreaming

import org.apache.spark.sql.SparkSession

object createDataFrameOrDataset {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("createDataFrameOrDataset")
      .master("local[*]")
      .getOrCreate()

    val dataFrame = sparkSession.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val query = dataFrame.writeStream
      .format("console")
      .outputMode("update")
      .start()

    query.awaitTermination()

    dataFrame.isStreaming

    dataFrame.printSchema()



  }
}
