package com.newegg.ec.rdd

import java.security.Key

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

case class request(key: String, value : String)
/**
 * Created by ricdong on 15-4-27.
 */
object ParquetTest {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Parquet Test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext._
//    import sqlContext.createSchemaRDD

    val rdd = sc.textFile("/Users/ricdong/Downloads/page.txt", 2)

    val rdds : RDD[request] = rdd.map(_.split("\001")).map(x => request(x(0), x(1)))
    rdd.collect().foreach(println(_))

//    rdds.saveAsParquetFile("")


  }
}
