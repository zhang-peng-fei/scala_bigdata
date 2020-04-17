package com.zhangpengfei.scala

object Demo1 {

  def main(args: Array[String]): Unit = {
    for (i <- 1 to 9; j <- 1 to i; r = s"$j*$i=${i * j}\t") yield {
      if (j == i) s"$r\n"
      else r
    }.foreach(print);

    for (i <- 1 to 9; j <- 1 to i; r = s"$j*$i=${i * j} \t") yield {
      if (i == j) s"$r\n"
      else r
    }.foreach(print)
  }
}
