package com.zhangpengfei.spark_demo.demo.structredstreaming

import com.zhangpengfei.util.CommUtils
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.types.StructType

object StartStreamingQueries {
  def main(args: Array[String]): Unit = {
    val basePath = CommUtils.getBasicPath

    val sparkSession = SparkSession.builder()
      .appName("StartStreamingQueries")
      .master("local[*]")
      .getOrCreate()

    sparkSession.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit =
        println("Query Stat ID:" + event.id)

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit =
        println("Query terminated: " + event.progress)

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit =
        println("Query made progress: " + event.id)
    })

    val userFrame = sparkSession.readStream
      .schema(new StructType().add("name", "string")
        .add("gender", "string")
        .add("age", "integer")
        .add("time", "string"))
      .option("seq", ",")
      .csv(basePath + "fileDir/csv/user")

    val aggDF = userFrame.select("name", "gender").where("age = 1")
//    val aggDF = userFrame.groupBy("age").count()


    val query = aggDF.writeStream
      .format("console")
      //      .queryName("table")
      .outputMode("update")
      //        .option("checkpointLocation", basePath + "fileDir/csv/res")
      //        .option("path", basePath + "fileDir/csv/res")
      .start()
    query.awaitTermination()

    //    val frame = sparkSession.sql("select * from table").show()

    aggDF.foreach(print(_))
    println(query.id) // get the unique identifier of the running query that persists across restarts from checkpoint data
    println(query.runId) // get the unique id of this run of the query, which will be generated at every start/restart
    println(query.name) // get the name of the auto-generated or user-specified name
    println(query.explain()) // print detailed explanations of the query
    println(query.stop()) // stop the query
    println(query.awaitTermination()) // block until query is terminated, with stop() or with error
    println(query.exception) // the exception if the query has been terminated with error
    println(query.recentProgress) // an array of the most recent progress updates for this query
    println(query.lastProgress) // th

    println(sparkSession.streams.active.length)
  }
}
