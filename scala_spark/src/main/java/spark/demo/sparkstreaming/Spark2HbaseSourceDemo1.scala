package spark.demo.sparkstreaming

import org.apache.hadoop.hbase.client.{HBaseAdmin, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableInputFormat
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * mapred代表的是hadoop旧API，而mapreduce代表的是hadoop新的API。
  */
object Spark2HbaseSourceDemo1 {

  def main(args: Array[String]): Unit = {
    // 屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder().appName("SparkHBaseRDD").getOrCreate()
    val sc = spark.sparkContext

    val tablename = "SparkHBase"

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "localhost") //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181") //设置zookeeper连接端口，默认2181
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    // 初始化job，TableOutputFormat 是 org.apache.hadoop.hbase.mapred 包下的
    val jobConf = new JobConf(hbaseConf)
    jobConf.setOutputFormat(classOf[org.apache.hadoop.hbase.mapred.TableOutputFormat])

    val indataRDD = sc.makeRDD(Array("2,jack,16", "1,Lucy,15", "5,mike,17", "3,Lily,14"))
    /**
      * 方法一
      * 使用 saveAsHadoopDataset 写入数据
      */
    val rdd = indataRDD.map(_.split(',')).map { arr =>
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据 须用 org.apache.hadoop.hbase.util.Bytes.toBytes 转换
       * Put.addColumn 方法接收三个参数：列族，列名，数据*/
      val put = new Put(Bytes.toBytes(arr(0)))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes(arr(1)))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes(arr(2)))
      (new ImmutableBytesWritable, put)
    }
    rdd.saveAsHadoopDataset(jobConf)

    /**
      * 方法二
      * 使用 newAPIHadoopRDD 读取数据
      */
    // 如果表不存在，则创建表
    val admin = new HBaseAdmin(hbaseConf)
    if (!admin.isTableAvailable(tablename)) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tablename))
      admin.createTable(tableDesc)
    }

    //读取数据并转化成rdd TableInputFormat 是 org.apache.hadoop.hbase.mapreduce 包下的
    val hBaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[org.apache.hadoop.hbase.mapreduce.TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    hBaseRDD.foreach { case (_, result) =>
      //获取行键
      val key = Bytes.toString(result.getRow)
      //通过列族和列名获取列
      val name = Bytes.toString(result.getValue("cf1".getBytes, "name".getBytes))
      val age = Bytes.toString(result.getValue("cf1".getBytes, "age".getBytes))
      println("Row key:" + key + "\tcf1.Name:" + name + "\tcf1.Age:" + age)
    }
    admin.close()

    /**
      * 方法三
      * Spark DataFrame 通过 Phoenix 读写 HBase
      */
    val url = s"jdbc:phoenix:localhost:2181"
    val dbtable = "PHOENIXTEST"

    //spark 读取 phoenix 返回 DataFrame 的 第一种方式
    val rdf = spark.read
      .format("jdbc")
      .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
      .option("url", url)
      .option("dbtable", dbtable)
      .load()
    rdf.printSchema()

    //spark 读取 phoenix 返回 DataFrame 的 第二种方式
    val df = spark.read
      .format("org.apache.phoenix.spark")
      .options(Map("table" -> dbtable, "zkUrl" -> url))
      .load()
    df.printSchema()

    //spark DataFrame 写入 phoenix，需要先建好表
    df.write
      .format("org.apache.phoenix.spark")
      .mode(SaveMode.Overwrite)
      .options(Map("table" -> "PHOENIXTESTCOPY", "zkUrl" -> url))
      .save()
    spark.stop()
  }
}
