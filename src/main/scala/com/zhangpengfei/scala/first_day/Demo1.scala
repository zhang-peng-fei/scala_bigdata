package com.zhangpengfei.scala.first_day

object Demo1 {

  def main(args: Array[String]): Unit = {

    /**
      * 操作符
      * scala中，符号越靠上优先级越高：
      */
//    var var1 = 2; // var1: Int = 2
//    var var16 = 2 + 3; // var2: Int = 5
//    var var3 = 2.+(3); // var3: Int = 5
    val t1 = 2.+(3).*(5) //> res23: Int = 25
    val t2 = 2 + (3) * (5) //> res24: Int = 17
    val t3 = 2 + 3 * 5 //> res25: Int = 17

    /**
      * 大数字操作
      */
    var v6 = BigInt(2) // scala.math.BigInt = 2
    v6.pow(10) // scala.math.BigInt = 1024  2的10次方
    var v7 = BigDecimal(2.0) // scala.math.BigDecimal = 2.0

    /**
      * 数据类型
      */
    var v1 = 4
    //每种数据类型都提供了很多方法供调用，所以scala的数据类型不同于java的基本类型
    println(v1.<(8)) // res0: Boolean = true
    //0000 1010
    //0000 0101
    v1.&(10)
    v1.%(2) // res8: Int = 0
    v1.toShort
    v1.toLong
    v1.toString
    v1.toDouble
    v1.compare(12) // res2: Int = -1
    v1.to(20) // res3: scala.collection.immutable.Range.Inclusive = Range(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
    v1 to 20
    v1 to(20, 2) // res5: scala.collection.immutable.Range.Inclusive = Range(2, 4, 6, 8, 10, 12, 14, 16, 18, 20)
    v1 to 10 by 2
    v1.until(20)
    v1 until 20 //scala.collection.immutable.Range = Range(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19)

    println(Int.MaxValue) //> res0: Int(2147483647) = 2147483647


    //String三引号用法
    //需要转义
    var var5 = "ab\\c";
    //不需要转义
    var var4 =
      """ab\c""";


    /**
      * 字符串操作
      */
    var v4 = "hello world!"
    //    v4.foreach(x => print(x))
    //    v4.filter { x => x != 'l' }.foreach(x=>print(x))
    //    v4.distinct.foreach(x=>print(x))
    print(v4.exists { x => x == 'p' })
    //    v4.drop(2).foreach(x=>print(x))
    //    v4.dropRight(2).foreach(x=>print(x))
    //    print(v4 * 3)
    //    print(v4.min)
    //    v4.mkString(",").foreach(x => print(x))

    /**
      * 常量定义
      * 一旦被赋值就不能再进行修改
      */
    val v11 = 100;
    //    v11 = 200;
    /**
      * 三种变量复制方式
      */
    var var1 = 100;
    var var2: Int = 100;
    var var3: java.lang.String = "hello";
    //    注：scala可以自动根据值的类型推断变量/常量的类型，所以不写类型声明也可以
    //变量修改
    var2 = 200;
    //    var3 = "world";

  }
}
