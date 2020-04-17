package com.zhangpengfei.util

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

object HbaseUtil {
//  已过时
//  val conf = new HBaseConfiguration()
  val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", "192.168.78.135")
  val connection = ConnectionFactory.createConnection(conf)
}
