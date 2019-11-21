package com.zhangpengfei.util

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

object HbaseUtil {
  val conf = new HBaseConfiguration()
  conf.set("hbase.zookeeper.quorum", "192.168.78.135")
  val connection = ConnectionFactory.createConnection(conf)
}
