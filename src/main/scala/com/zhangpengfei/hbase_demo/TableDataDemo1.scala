package com.zhangpengfei.hbase_demo

import com.zhangpengfei.util.HbaseUtil
import org.apache.commons.lang.time.StopWatch
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.client.coprocessor.{AggregationClient, LongColumnInterpreter}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

class TableDataDemo1(conf: HBaseConfiguration, admin: Admin) {
  /**
    * 为什么利用协处理器后速度会如此之快？
    *
    * Table注册了Coprocessor之后，在执行AggregationClient的时候，会将RowCount分散到Table的每一个Region上，Region内RowCount的计算，是通过RPC执行调用接口，由Region对应的RegionServer执行InternalScanner进行的。
    *
    * 因此，性能的提升有两点原因:
    *
    * 1.分布式统计。将原来客户端按照Rowkey的范围单点进行扫描，然后统计的方式，换成了由所有Region所在RegionServer同时计算的过程。
    *
    * 2.使用了在RegionServer内部执行使用了InternalScanner。这是距离实际存储最近的Scanner接口，存取更加快捷。
    *
    * @param tableName
    */
  def countData(tableName: String): Unit = {
    val table = new HTable(conf, tableName)
    val descriptor = table.getTableDescriptor
    // 定义协调处理器名称
    val aggre = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation"
    // 如果表没有该协同处理器，先关表，addCoprocessor，开启表
    if (!descriptor.hasCoprocessor(aggre)) {
      val tbn = TableName.valueOf(tableName)
      admin.disableTable(tbn)
      descriptor.addCoprocessor(aggre)
      admin.modifyTable(TableName.valueOf(tableName), descriptor)
      admin.enableTable(tbn)
    }
    // 创建处理器客户端
    val client = new AggregationClient(conf)
    // 计算 count 时间
    val watch: StopWatch = new StopWatch()
    watch.start()
    // 开始 rowCount 操作
    val count = client.rowCount(table, new LongColumnInterpreter, new Scan())
    watch.stop()
    // 打印
    println(watch.getTime)
    println(tableName + " count si : " + count)
  }

  def deleteDataByRowKey(tableName: String, row: String): Unit = {
    val table = new HTable(conf, tableName)

    val delete = new Delete(row.getBytes())

    table.delete(delete)
  }


  def getDataByRowKey(tableName: String, row: String): Unit = {
    // 创建 get 对象，构造方法：rowKey
    val get = new Get(row.getBytes())
    // 创建 Htable 对象，构造方法：conf配置对象，tableName
    val table = new HTable(conf, tableName)
    // get 方法，从表中获取数据
    val result = table.get(get)
    if (result.size() != 0) {
      // 根据列族获取数据
      val map = result.getFamilyMap("f1".getBytes())
      val iter = map.entrySet().iterator()
      // 开始遍历
      while (iter.hasNext) {
        val res = iter.next()
        val key = new String(res.getKey)
        val value = new String(res.getValue)
        println("rowKey is : " + row + ", key is : " + key + ", value is : " + value)

      }
    } else {
      println("数据查询为空")
    }
  }

  def scanData(tableName: String): Unit = {
    // 创建 scan 对象
    val scan = new Scan
    // 创建表对象，构造方法：配置对象，表名
    val table = new HTable(conf, tableName)
    // 获取 scanner 对象
    val scanner: ResultScanner = table.getScanner(scan)
    // 从 scanner 对象中取所有结果
    val result = scanner.next()
    // 遍历全部结果
    while (result != null) {
      // 获取 rowKey
      val row = new String(result.getRow)
      // 获取 rowKey 下所有数据，返回的结构是 NavigableMap<byte[], byte[]>
      val map = result.getFamilyMap("f1".getBytes)
      val iter = map.entrySet().iterator()
      // 开始遍历 map 结果数据
      while (iter.hasNext) {
        val entry = iter.next()
        val key = new String(entry.getKey)
        val value = new String(entry.getValue)
        println("rowKey is : " + row + ", key is : " + key + ", value is : " + value)
      }
    }
  }

  def insertData(tableName: String): Unit = {
    // 新建表对象
    val table = new HTable(conf, tableName)
    // 造假数据开始
    for (a <- 51 to 150) {
      // 创建 put 对象，用来存放数据，构造方法里放 rowKey 的值
      val put = new Put(("rowKey" + a).getBytes())
      for (b <- 1 to 10) {
        val key = ("rowKey" + a + "-key" + b).getBytes()
        val value = ("rowKey" + a + "-value" + b).getBytes()
        // addColumn 方法参数：列族，key，value
        put.addColumn("f1".getBytes(), key, value)
      }
      // put 数据
      table.put(put)
    }
  }


}


object TableDataDemo1 {

  def main(args: Array[String]): Unit = {
    // 获取 hbase 配置
    val conf = HbaseUtil.conf
    // 获取连接
    val connection = HbaseUtil.connection
    // 获取管理员对象
    val admin = connection.getAdmin()

    val demo = new TableDataDemo1(conf, admin)
    demo.insertData("tbs")
    demo.scanData("tbs")
    demo.getDataByRowKey("tbs", "rowKey1")
    demo.deleteDataByRowKey("tbs", "rowKey9")
    demo.countData("tbs")
  }
}
