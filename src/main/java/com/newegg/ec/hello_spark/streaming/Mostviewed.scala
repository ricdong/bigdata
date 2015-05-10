package com.newegg.ec.hello_spark.streaming

import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.{Time, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkContext, SparkConf}

case class request(page: String, province: String, city: String)


/**
 * Created by ricdong on 15-4-24.
 */
object Mostviewed {

  def main(args: Array[String]) {
    val args = Array("192.168.1.108", "test", "us_truesight", "10") // production
    // val args = Array("10.16.40.35", "mygrp-rd29-test", "us_truesight", "2") // local test

    val Array(zkQuorum, group, topics, numThreads) = args

    val conf = new SparkConf().setAppName("most item viewed from truesight page").setMaster("local[2]")

   LoggerManager.setStreamingLogLevels(Level.ERROR)

    val ssc = new StreamingContext(conf, Seconds(3))
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    // val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_ONLY).map(_._2)
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    // lines.map(x => ("http:", x.split("\001")(0), x.split("\001").length)).print()

    // val https = lines.map(_.split("\001")).map(x => request(x(6), x(74), x(75)))
    val https = lines.map(_.split("\001")).filter(x => (x(75) != null && !x(75).equals(""))).map(x => request(x(6), x(74), x(75)))
    // https.print(100)

    val winDStream : DStream[request] = https.window(Seconds(30 * 60), Seconds(60))

    winDStream.foreachRDD((rdd: RDD[request], time: Time) => {

      // Get the singleton instance of SQLContext
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._

      // Convert RDD[String] to RDD[case class] to DataFrame
      val wordsDataFrame = rdd.toDF()

      // Register as table
      wordsDataFrame.registerTempTable("requestfromwhere")

      // Do word count on table using SQL and print it
      val wordCountsDataFrame =
        sqlContext.sql("select city, count(city) as t from requestfromwhere group by city order by t desc limit 10")
      println(s"========= $time =========")
      wordCountsDataFrame.show()
    })

      ssc.start()
    ssc.awaitTermination()
  }
}

/** Lazily instantiated singleton instance of SQLContext */
object SQLSingleton {

  @transient private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}
