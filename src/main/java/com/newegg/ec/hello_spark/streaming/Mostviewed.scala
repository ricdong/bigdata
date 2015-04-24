package com.newegg.ec.hello_spark.streaming

import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.{Time, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField

import scala.collection.mutable.ArrayBuffer
case class request(cs_uri_stem: String, cs_uri_query: String, c_ip: String)


/**
 * Created by ricdong on 15-4-24.
 */
object Mostviewed {

  def main(args: Array[String]) {
    val args = Array("192.168.1.108", "mygrp-rd29-test", "us_truesight", "2") // production
    // val args = Array("10.16.40.35", "mygrp-rd29-test", "us_truesight", "2") // local test

    val Array(zkQuorum, group, topics, numThreads) = args

    val conf = new SparkConf().setAppName("most item viewed from truesight page").setMaster("local[2]")

   // LoggerManager.setStreamingLogLevels(Level.ERROR)

    val ssc = new StreamingContext(conf, Seconds(30))
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    // val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_ONLY).map(_._2)
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    val https: DStream[request] = lines.map(_.split("\u0001")).filter(x => (x.length > 19)).filter(x => x(7).equals("/Product/Product.aspx")).map { x => request(x(7), x(8), x(19))}

    https.print()
    //val winDStream : DStream[http_request] = https.window(Seconds(30 * 60), Seconds(2 * 60))

    ssc.start()
    ssc.awaitTermination()
  }
}
