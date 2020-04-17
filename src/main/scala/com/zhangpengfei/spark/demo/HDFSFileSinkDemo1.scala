package com.zhangpengfei.spark.demo

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.DefaultCodec
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job => NewAPIHadoopJob}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object HDFSFileSinkDemo1 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName(HDFSFileSourceDemo1.getClass.toString)
      .setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    //    sc.setLogLevel("ERROR")
    val rdd = sc.parallelize(List(("1", "one"), ("2", "two")))

    // 设置缓存级别
    rdd.persist(StorageLevel.MEMORY_ONLY)


    val hdfsPath = "hdfs://192.168.78.135:9000/tmp/FileSinkDemo1/"
    val hdfs = FileSystem.get(new java.net.URI(hdfsPath), new Configuration())
    val expath = new Path("hdfs://192.168.78.135:9000/tmp/FileSinkDemo1/")
    if (hdfs.exists(expath)) {
      println("delete exits files")
      hdfs.delete(expath, true)
    }

    rdd.saveAsTextFile(hdfsPath + "saveAsTextFile1")
    rdd.saveAsTextFile(hdfsPath + "saveAsTextFile2", classOf[DefaultCodec])


    rdd.saveAsHadoopFile[TextOutputFormat[String, String]](hdfsPath + "saveAsHadoopFile1")
    rdd.saveAsHadoopFile[TextOutputFormat[String, String]](hdfsPath + "saveAsHadoopFile2", classOf[DefaultCodec])
    rdd.saveAsHadoopFile(hdfsPath + "saveAsHadoopFile3", classOf[String], classOf[String], classOf[TextOutputFormat[String, String]], classOf[DefaultCodec])
    rdd.saveAsHadoopFile(hdfsPath + "saveAsHadoopFile4", classOf[String], classOf[String], classOf[TextOutputFormat[String, String]], new JobConf(), Some(classOf[DefaultCodec]))

    rdd.saveAsObjectFile(hdfsPath + "saveAsObjectFile1")


    rdd.saveAsNewAPIHadoopFile[org.apache.hadoop.mapreduce.lib.output.TextOutputFormat[String, String]](hdfsPath + "saveAsNewAPIHadoopFile1")
    rdd.saveAsNewAPIHadoopFile(hdfsPath + "saveAsNewAPIHadoopFile2", classOf[String], classOf[String], classOf[org.apache.hadoop.mapreduce.lib.output.TextOutputFormat[String, String]])


    rdd.saveAsSequenceFile(hdfsPath + "saveAsSequenceFile1")


    // 通过 SparkContext 上下文获取 hadoopConfiguration 配置
    val hadoopConfiguration: Configuration = sc.hadoopConfiguration
    // saveAsHadoopFile 这个方法最终调用的是 saveAsHadoopDataset 方法
    val hadoopConf = new JobConf(hadoopConfiguration)
    hadoopConf.setOutputKeyClass(classOf[String])
    hadoopConf.setOutputValueClass(classOf[String])
    hadoopConf.setCompressMapOutput(true)
    hadoopConf.set("mapred.output.compress", "true")
    hadoopConf.setMapOutputCompressorClass(classOf[DefaultCodec])
    hadoopConf.set("mapred.output.compression.codec", classOf[DefaultCodec].getCanonicalName)
    hadoopConf.set("mapred.output.compression.type", CompressionType.BLOCK.toString)
    hadoopConf.setOutputCommitter(classOf[FileOutputCommitter])
    val outputPath = new Path(hdfsPath + "saveAsHadoopDataset1")

    /**
      * 不要在Spark之外使用这个类。它的目的是作为一个内部实用程序。在将来的版本中可能会更改或删除。
      * SparkHadoopWriter.createPathFromString(hdfsPath, hadoopConf)
      */
    //    SparkHadoopWriter.createPathFromString(hdfsPath, hadoopConf)
    FileOutputFormat.setOutputPath(hadoopConf, outputPath)

    rdd.saveAsHadoopDataset(hadoopConf)


    // 同上：saveAsHadoopDataset方法，saveAsNewAPIHadoopFile 这个方法最终调用的是 saveAsNewAPIHadoopDataset 方法
    val job = NewAPIHadoopJob.getInstance(hadoopConfiguration)
    job.setOutputFormatClass(classOf[org.apache.hadoop.mapreduce.lib.output.TextOutputFormat[String, String]])
    job.setOutputKeyClass(classOf[String])
    job.setOutputValueClass(classOf[String])
    val jobConfiguration = job.getConfiguration
    jobConfiguration.set("mapred.output.dir", hdfsPath + "saveAsNewAPIHadoopDataset1")
    rdd.saveAsNewAPIHadoopDataset(jobConfiguration)

    rdd.map(x => (x._1, x._2))
      .foreach(println(_))


  }
}
