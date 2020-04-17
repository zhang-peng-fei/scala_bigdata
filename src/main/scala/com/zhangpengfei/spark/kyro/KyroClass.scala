package com.zhangpengfei.spark.kyro

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.SparkContext
import org.apache.spark.serializer.KryoRegistrator


class KryoDemo1 {}

class KyroClass extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[KryoDemo1])
  }
}
object KryoClass {
  def main(args: Array[String]): Unit = {
    System.setProperty("spark.serializer", "spark.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "com.zhangpengfei.spark_demo.kyro.KyroClass")
    val context = new SparkContext()
  }
}
