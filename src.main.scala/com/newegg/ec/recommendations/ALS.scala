/**
 *
 */
package com.newegg.ec.recommendations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS
import org.apache.log4j.{Level, Logger}

object ALS {

  def main(args: Array[String]) {
    
    val args = Array("resource/ml-100k/u1.base")

    if (args.length < 1) {
      System.err.println("Usage: ALS <file>")
      System.exit(-1)
    }
    
    val conf:SparkConf = new SparkConf().setAppName("Als Test") 
    conf.setMaster("local[2]") 
    
    Logger.getRootLogger.setLevel(Level.WARN)
    
    val sc:SparkContext = new SparkContext(conf)
    val data = sc.textFile(args(0), 1) 
    
    // show data 
    // data.foreach { x => println(x) } 
    
    val ratings = data.map(_.split("\t") match { case Array(user, item, rate, ts) =>  
    Rating(user.toInt, item.toInt, rate.toDouble)}).cache()
    
    // ratings.foreach { x => println(x) } 
    
    val rank = 10 
    val numOfIter = 30 
    
    val model = new ALS().setIterations(numOfIter).setRank(rank).run(ratings) 
    
    val usersProducts = ratings.map { case Rating(user, product, rate) =>  
     (user, product) } 
    
    usersProducts.count()
    
    val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) =>   
     ((user, product), rate) }

    val ratesAndPreds = ratings.map {
      case Rating(user, product, rate) =>
        ((user, product), rate)
    }.join(predictions)
    
    ratesAndPreds.top(100).foreach(println)
    
    // val model = ALS.train(ratings, factor, numOfInter, 0.01)  
  }
}