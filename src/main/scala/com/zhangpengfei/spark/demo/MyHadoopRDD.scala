package com.zhangpengfei.spark.demo

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, JobConf, TextInputFormat}
import org.apache.spark.{SparkConf, SparkContext}

object MyHadoopRDD {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(HDFSFileSourceDemo1.getClass.toString)
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
//    sc.setLogLevel("ERROR")

    val jobConf = new JobConf
    FileInputFormat.setInputPaths(jobConf, new Path("hdfs://192.168.78.135:9000/user/hive/warehouse/testhivedrivertable/a.txt"));
    val hadoopRDD = sc.hadoopRDD(jobConf, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 2)
    println("hadoopRDD is Running.............")
    hadoopRDD.foreach("hadoopRDD的结果为：" + println(_))

    val configuration = new Configuration
    configuration.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR, "hdfs://192.168.78.135:9000/user/hive/warehouse/testhivedrivertable/a.txt")
    configuration.addResource("hdfs://192.168.78.135:9000/user/hive/warehouse/testhivedrivertable/a.txt")
    val newAPIHadoopRDD = sc.newAPIHadoopRDD(configuration, classOf[org.apache.hadoop.mapreduce.lib.input.TextInputFormat], classOf[LongWritable], classOf[Text])
    println("newAPIHadoopRDD is Running............")
    newAPIHadoopRDD.foreach("newAPIHadoopRDD的结果是：" + println(_))
  }
}
