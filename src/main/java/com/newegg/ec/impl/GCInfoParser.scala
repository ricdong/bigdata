package com.newegg.ec.impl

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object GCInfoParser {

  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("Test Split")
    conf.setMaster("local[2]") 
    
    val sc = new SparkContext(conf)
    
    val data = sc.textFile("resource/gtimes.txt", 2) 
    // sort only by a column self. 
    // data.map(_.split("=")).map { x => x(1) }.map(_.split(" ")).map( x => x(0)).sortBy(c => c, false, 1).foreach { println }
    
    // sort by a column 
    /**
     * (3.79, CMS: abort preclean due to time 2015-04-01T14:08:27.624-0700: 13219107.696: [CMS-concurrent-abortable-preclean: 3.385/5.024 secs] [Times: user=3.79 sys=0.13, real=5.03 secs] )
      (3.78, CMS: abort preclean due to time 2015-04-01T15:47:50.297-0700: 13225070.370: [CMS-concurrent-abortable-preclean: 2.816/5.022 secs] [Times: user=3.78 sys=0.04, real=5.02 secs] )
      (3.60, CMS: abort preclean due to time 2015-04-01T16:05:26.835-0700: 13226126.908: [CMS-concurrent-abortable-preclean: 3.214/5.118 secs] [Times: user=3.60 sys=0.10, real=5.12 secs] )
      (3.58, CMS: abort preclean due to time 2015-04-01T21:07:36.738-0700: 13244256.810: [CMS-concurrent-abortable-preclean: 3.244/5.148 secs] [Times: user=3.58 sys=0.11, real=5.15 secs] )
      ... 
     */
    data.map{ x => (x.split("=")(1).split(" ")(0) , x)}.sortByKey(false).top(100).foreach(println) 
  }
}