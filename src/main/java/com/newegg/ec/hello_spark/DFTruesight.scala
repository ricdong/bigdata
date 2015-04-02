package com.newegg.ec.hello_spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import kafka.utils.Logging
import scala.collection.mutable.ArrayBuffer

// Data from memory or Kafka 
// SQL on memory

object DFTruesight extends Logging  {

  def main(args : Array[String]) {
   val conf = new SparkConf().setAppName("Dataframe in Memory")
   conf.setMaster("local[2]") 
   val sc = new SparkContext(conf)
   val sqlContext = new SQLContext(sc)
   
   import org.apache.spark.sql._
   import sqlContext._ 
   
   // val schemaString: String = "c_ip_int64,c_ip,org,countrycode,countryname,region,cityname,zipcode,latitude,longitude,statename"
   val schemaString: String = "c_ip_int64,c_ip,org,countrycode,countryname,region"
    
   val schema = StructType(schemaString.split(",").map(f => StructField(f, StringType, true)))
   
   val consumer = new KafkaConsumer(); 
   consumer.onStart()
   
   val buffer = new ArrayBuffer[String]
   
   while(true) {
      val line = consumer.queue.take()
    		  debug("line " + line)
      buffer += line
      
      if(buffer.size >= 100) {
        info("buffer size " + buffer.size)
        
        val rdd = sc.parallelize(buffer).map (_.split(",")).map (x => Row(x(0), x(1), x(2), x(3), x(4), x(5))) 
        val peopleDF = sqlContext.createDataFrame(rdd, schema) 
   
        peopleDF.show()
        
        peopleDF.foreach { x => println(x.get(0)) } 
        
        info("total count " + peopleDF.count().toInt)
        buffer.clear()
      }
   }
   
  }
  
}