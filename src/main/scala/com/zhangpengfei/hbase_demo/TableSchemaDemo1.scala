package com.zhangpengfei.hbase_demo

import com.zhangpengfei.util.HbaseUtil
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.Admin


class TableSchemaDemo1(val tableName: String) {
  val tb = TableName.valueOf(tableName)


  def dropTable(admin: Admin) = {

    if (admin.tableExists(this.tb)) {
      admin.disableTable(this.tb)
      admin.deleteTable(this.tb)
    } else
      println("表不存在，请确认表名")
  }

  def describeTable(admin: Admin) = {

    val descriptor = admin.getTableDescriptor(TableName.valueOf(tableName))
    val families = descriptor.getFamilies
    println(families)
  }

  def createTable(admin: Admin) = {

    if (admin.tableExists(this.tb)) {
      println("表已存在，请换个表名重新建表")
    } else {
      val descriptor = new HTableDescriptor
      descriptor.setName(TableName.valueOf(tableName))
      descriptor.addFamily(new HColumnDescriptor("f1"))
      admin.createTable(descriptor)
      println("create table done")
    }
  }

  def listTables(admin: Admin) = {
    val names = admin.listTableNames()
    names.foreach(println(_))
  }


  def listTables(): Unit = {

  }


  override def toString = s"TableSchemaDemo1($tableName)"
}

object TableSchemaDemo1 {
  def main(args: Array[String]): Unit = {

    // 获取 hbase 配置
    val conf = HbaseUtil.conf
    // 获取连接
    val connection = HbaseUtil.connection
    // 获取管理员
    val admin = connection.getAdmin()

    val demo = new TableSchemaDemo1("tbs")
    demo.listTables(admin)
    demo.createTable(admin)
    demo.describeTable(admin)
//    demo.dropTable(admin)

    println(demo.toString)
  }
}
