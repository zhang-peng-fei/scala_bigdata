package spark.demo

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

object HDFSFileSourceDemo1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(HDFSFileSourceDemo1.getClass.toString)
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
//    sc.setLogLevel("ERROR")

    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data, 2)
    val value1 = distData.reduce((a, b) => a + b)


    val distFile = sc.textFile("F:\\workSpace\\IdeaProjects\\b_learning\\scala_flink\\src\\main\\resources\\fileDir\\people.txt", 2)
    val value2 = distFile.map(s => s.length)
    value2.persist()
    val totalLength = value2.reduce((a, b) => a + b)

    //    可以读取包含多个小文件的目录，并以（filename，content）键值对的形式返回，与 textFile 相反，每个文件每行作为一条记录返回。
    sc.wholeTextFiles("F:\\workSpace\\IdeaProjects\\b_learning\\scala_flink\\src\\main\\resources\\fileDir", 2)
    //      .foreach(println(_))


    sc.sequenceFile[Int, String]("")


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


    distData.map(func1("=="))

    /**
      * Wrong: Don't do this!!
      */
    //    var counter = 0
    //    distData.foreach(x => x += counter)

    val broadcastData = sc.broadcast(data)
    broadcastData.value.map(x => println("广播变量结果值：" + x))

    val accumilator = sc.longAccumulator("countData")
    distData.foreach(x => accumilator.add(x))
    println("累加器结果：" + accumilator.value)


    val accumulatorV = new VectorAccumulatorV2
    sc.register(accumulatorV, "accumulatorV")
    distData.foreach(x => accumulatorV.add(x))
    println("自定义累加器的结果是：" + accumulatorV.value)
  }

  def func1(str: String) = {
    "hello world!" + str
  }

  class MyClass {
    val field = "Hello"

    def func1(str: String) = {
      str.toUpperCase()
    }

    def doStuff(rdd: RDD[String]) = {
      rdd.map(func1)
    }

    def doStuff1(rdd: RDD[String]) = {
      val field_ = this.field
      rdd.map(x => field_ + x)
    }
  }

  /**
    * 自定义 计数器
    */
  class VectorAccumulatorV2 extends AccumulatorV2[Long, Long] {
    private var _sum = 0L
    private var _count = 0L

    def sum = _sum

    def count = _count

    override def reset(): Unit = {
      _sum = 0L
      _count = 0L
    }

    override def isZero: Boolean = {
      true
    }

    override def copy(): AccumulatorV2[Long, Long] = {
      val v = new VectorAccumulatorV2
      v._count = this._count
      v._sum = this._sum
      v
    }

    override def add(v: Long): Unit = {
      _sum += v
      _count += 1
    }

    override def merge(other: AccumulatorV2[Long, Long]): Unit = other match {
      case o: VectorAccumulatorV2 => {
        _count += o.count
        _sum += o.sum
      }
      case _ =>
        throw new UnsupportedOperationException(
          s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }

    override def value: Long = {
      sum
    }
  }

}
