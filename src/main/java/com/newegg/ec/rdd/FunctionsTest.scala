package com.newegg.ec.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object FunctionsTest {

  def main(args : Array[String]) {
    
    val conf = new SparkConf().setAppName("functions Test")
    conf.setMaster("local[2]") // just for test
    val sc = new SparkContext(conf)
    
    val rdd = sc.textFile("resource/words", 2)
    // rdd.flatMap(_.split(" ")).countByValue().foreach(print)
    
    // Creating a pair RDD using the first word as the key in Scala
    val pairs = rdd.map( x => (x.split(" ")(0), x))
    
    val v = pairs.filter{case (key, value) => key.equals("002")}
    v.foreach(println)
    
    val arr = Array("panda 0", "pink 3", "pirate 3", "panda 1", "pink 5")
    val pairs2 = sc.parallelize(arr, 1).map {_.split(" ")}.map ( x => (x(0), x(1)) )
    pairs2.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).foreach(println)
    
    // val data = Seq(("apple",4), ("panda", 2), ("grace", 3), ("panda", 3,45,"unitprice"))
   // sc.parallelize(data, 2).foreach(println)
    
    val data = Seq(("a", 3), ("b", 4), ("a", 1))
    sc.parallelize(data).reduceByKey((x, y) => x + y).foreach(println)
    
    // group by 
    sc.parallelize(data, 2).groupByKey().foreach(println)

    val a = sc.parallelize(1 to 9, 3)
    def myfunc(a: Int): Int =
      {
        a % 2
      }
    a.groupBy(x => myfunc(x), 3).foreach(println)
  }
}