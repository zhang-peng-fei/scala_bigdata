package spark.demo.sparkstreaming

import com.zhangpengfei.util.CommUtils
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}

object FileSourceDemo1 {

  private val LOGGER: Logger = LoggerFactory.getLogger(FileSourceDemo1.getClass)
  val LOG_TAG = "================================"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(FileSourceDemo1.getClass.getName)
    //      .set("spark.driver.allowMultipleContexts", "true")//spark.driver.allowMultipleContexts = true
    val sparkContext = new SparkContext(sparkConf)
    val streamingContext = new StreamingContext(sparkContext, Seconds(5))

    // F:\workSpace\IdeaProjects\b_learning\scala_flink\src\main\resources\fileDir\people.txt
    val streamTextFile = streamingContext.textFileStream(CommUtils.filePath + "fileDir\\people.txt")
    LOGGER.info("streamTextFile" + LOG_TAG)
    streamTextFile.flatMap(x => x.split(" "))
      .print()


//    val fileStream = streamingContext.fileStream[String, String, org.apache.hadoop.mapreduce.lib.input.FileInputFormat[String, String]](CommUtils.filePath + "fileDir\\stu")
//    LOGGER.info("fileStream" + LOG_TAG)
//    fileStream.map(x => x._1)
//      .print()

//    streamingContext.textFileStream(CommUtils.filePath + "fileDir\\people.txt")
//      .flatMap(x => x.split(" "))
//      .print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
