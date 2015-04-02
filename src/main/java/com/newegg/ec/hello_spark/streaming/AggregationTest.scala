package com.newegg.ec.hello_spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object AggregationTest {

  def main(args : Array[String]) {
    
     val sc = new SparkContext(new SparkConf().setAppName("AggregationTest").setMaster("local[2]"))
    
    val data = Seq(("a", 8), ("b", 4), ("a", 1))
    
    val rdd = sc.parallelize(data).reduceByKey((x, y) => (x + y))    // Default parallelism
    
    rdd.foreach(println)

    val result = sc.parallelize(data).combineByKey(
      (v) => (v, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)).
      map { case (key, value) => (key, value._1 / value._2.toFloat) }
    result.collectAsMap().map(println(_))
  
    // sc.parallelize(data).reduceByKey((x, y) => x + y)    // Custom parallelism
    
  }
}