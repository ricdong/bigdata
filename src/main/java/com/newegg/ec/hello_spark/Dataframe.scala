package com.newegg.ec.hello_spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

object Dataframe {

  def main(args: Array[String]) {
    
    val sparkConf = new SparkConf().setAppName("Dataframe test")
    sparkConf.setMaster("local[2]") 
    
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    
    val df : DataFrame = sqlContext.jsonFile("/Users/ricdong/workspace/luna/hello-spark-1.3/resource/people.json")
    // show the content of the dataframe
    df.show()
    
    // show the schema of the dataframe
    df.printSchema()
    
    // select the name / age column 
    df.select("name", "age").show()
    
    // select only the "name" column 
    df.select("name").show()
    
    // select and filter 
    df.select("name", "age").filter(df("age") > 25).show()
    
    // group by the page column and find the max 
    df.groupBy("page").max("age").show() 
  }
}