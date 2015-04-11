package com.newegg.ec.hello_spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import kafka.utils.Logging
import scala.collection.mutable.ArrayBuffer

object DFInMemory {
 def main(args : Array[String]) {
   // data should be from kafka 
    
   val arr = Array("ric y dong 29", 
                   "cherry s yue 30", 
                   "json j xuan 30", 
                   "jessica j wang 25")
   // just print the content of the array 
   arr.foreach(print)
   
   val conf = new SparkConf().setAppName("Dataframe for truesight")
   conf.setMaster("local[2]") 
   val sc = new SparkContext(conf)
   val sqlContext = new SQLContext(sc)
   
   import org.apache.spark.sql._
   import sqlContext._ 
   
   val schemaString: String = "firstname middlename lastname birthday"
   
   // now we convert the array to dataframe in memory
    
   val schema = StructType(schemaString.split(" ").map(f => StructField(f, StringType, true)))
   
   val rdd = sc.parallelize(arr).map (_.split(" ")).map (x => Row(x(0), x(1), x(2), x(3))) 
   
   val peopleDF = sqlContext.createDataFrame(rdd, schema) 
   
   peopleDF.show()
   
   peopleDF.groupBy("birthday").count().show()
   
   
 }
}