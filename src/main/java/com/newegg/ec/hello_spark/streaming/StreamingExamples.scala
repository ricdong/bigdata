package com.newegg.ec.hello_spark.streaming
import org.apache.spark.Logging

import org.apache.log4j.{Level, Logger}

object StreamingExamples  extends Logging {
/** Set reasonable logging levels for streaming if the user has not configured log4j. */
  def setStreamingLogLevels(level : Level) {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      logInfo("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(level)
    }
  }
  
  def main(args : Array[String]) {
    setStreamingLogLevels(Level.INFO)
  }
}