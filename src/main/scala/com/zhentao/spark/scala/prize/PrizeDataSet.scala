package com.zhentao.spark.scala.prize
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

object PrizeDataSet extends App {
    val sparkSession = SparkSession.builder().appName("Join movie records with rating").getOrCreate();
    
    //val moviePair = sc.textFile(args(0)).map(_.split(",")).map(rec => (rec(0), rec(1) + "," + rec(2)))
    //val ratingPair = sc.textFile(args(1)).map(_.split(",")).map(rec => (rec(0), rec(1) + "," + rec(2) + "," + rec(3)))

    // use map() to convert to a Dataset of a specific class
    //var ds: Dataset[Person] = sparkSession.read.text("people.txt").map(row => parsePerson(row))
}