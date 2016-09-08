package com.zhentao.spark.scala.prize
import org.apache.spark.SparkContext._
import org.apache.spark.{ SparkConf, SparkContext }


object Prize {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Join movie records with rating")
    val sc = new SparkContext(conf)

    val moviePair = sc.textFile(args(0)).map(_.split(",")).map(rec => (rec(0), rec(1) + "," + rec(2)))
    val ratingPair = sc.textFile(args(1)).map(_.split(",")).map(rec => (rec(0), rec(1) + "," + rec(2) + "," + rec(3)))

    //join movie and rating
    val join = moviePair.join(ratingPair)

    //calculate average rating for moives with at least 2 ratings    
    val average = join.combineByKey(
      (v) => (v._2.split(",")(1).toInt, 1, v._1), //rating, count
      (acc: (Int, Int, String), v) => (acc._1 + v._2.split(",")(1).toInt, acc._2 + 1, v._1),
      (acc1: (Int, Int, String), acc2: (Int, Int, String)) => (acc1._1 + acc2._1, acc1._2 + acc2._2, acc1._3))
      .filter { case (key, value) => value._2 > 1 }.map { case (key, value) => (key, (value._1 / value._2.toFloat), value._3) }

    val sorted = average.sortBy({case (key, rating, yearAndTitle) => rating}, false)
      //average.takeOrdered(num)
    sorted.saveAsTextFile(args(2))
  }
}


