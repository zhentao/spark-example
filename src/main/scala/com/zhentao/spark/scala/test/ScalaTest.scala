package com.zhentao.spark.scala.test

object ScalaTest extends App {
  def foo(n: Int, v: Int) =
    for (
      i <- 0 until n;
      j <- i until n 
    ) yield Tuple2(i, j);
  foo(20, 32) foreach {
    case (i, j) =>
      println("(" + i + ", " + j + ")")
  }
}