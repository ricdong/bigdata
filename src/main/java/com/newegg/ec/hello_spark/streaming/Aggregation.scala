package com.newegg.ec.hello_spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.Minutes
import org.apache.spark.sql._
import org.apache.spark.streaming.dstream.DStream
import kafka.utils.Logging

case class http_request(c_ip :String, org:String)

object Aggregation extends Logging {

  def main(args: Array[String]) {
    
    val args = Array("192.168.1.108", "mygrp1", "us_truesight", "2")
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    sparkConf.setMaster("local[2]") 
    val ssc =  new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    //val words = lines.flatMap(_.split(","))
    
    // we define the schema  c_ip_int64,c_ip,org,countrycode,countryname,region
    val rdd : DStream[http_request] = lines.map(_.split(",")).map { x => http_request(x(1), x(2)) }
    
    val result = rdd.map { x => (x.c_ip, 1) }.reduceByKeyAndWindow((x, y) => (x + y), Seconds(10))
    
//    val wordCounts = words.map(x => (x, 1L))
//      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
  
}