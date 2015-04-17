package com.newegg.ec.hello_spark

import com.newegg.ec.hello_spark.streaming.StreamingExamples
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField

object DFInMemory {
 def main(args : Array[String]) {
   // data should be from kafka

   StreamingExamples.setStreamingLogLevels(Level.WARN)
    
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

   val data = Seq(("dong", 3), ("wang", 4), ("xuan", 1))

   val rdd2 = sc.parallelize(data).map(x => Row(x._1, x._2))

   val schema2 = StructType("name id".split(" ").map(f => StructField(f, StringType, true)))


   val df2 = sqlContext.createDataFrame(rdd2,schema2)

   peopleDF.join(df2, df2("name") === peopleDF("lastname"), "inner").show()
   
 }
}