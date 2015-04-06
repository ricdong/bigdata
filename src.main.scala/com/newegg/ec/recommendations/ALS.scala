/**
 *
 */
package com.newegg.ec.recommendations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.ALS  
import org.apache.spark.mllib.recommendation.Rating

object ALS {

  def main(args: Array[String]) {
    
    val args = Array("resource/ml-100k/u1.base")

    if (args.length < 1) {
      System.err.println("Usage: ALS <file>")
      System.exit(-1)
    }
    
    val conf:SparkConf = new SparkConf().setAppName("Als Test") 
    conf.setMaster("local[2]") 
    
    val sc:SparkContext = new SparkContext(conf)
    val data = sc.textFile(args(0), 1) 
    
    // show data 
    data.foreach { x => println(x) } 
  }
}