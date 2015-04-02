package com.newegg.ec.hello_spark

import scala.collection.JavaConversions._
import java.util.concurrent.LinkedBlockingQueue
import java.util.Properties
import kafka.consumer.ConsumerConfig
import kafka.consumer.Consumer
import kafka.consumer.Whitelist
import kafka.serializer.DefaultDecoder
import kafka.utils.Logging

class KafkaConsumer extends Logging {
  
  // val queue = new scala.collection.mutable.SynchronizedQueue[String]
  
  val queue = new LinkedBlockingQueue[String]

  val props = new Properties()

  props.put("group.id", "mygrp")

  props.put("zookeeper.connect", "192.168.1.108")
  //props.put("auto.offset.reset", "smallest")
  
  val config = new ConsumerConfig(props)
  val connector = Consumer.create(config)

  val filterSpec = new Whitelist("us_truesight")
  
  // info("setup:start topic=%s for zk=%s and groupId=%s".format(topic,zookeeperConnect,groupId))

  val stream = connector.createMessageStreamsByFilter(filterSpec, 1, new DefaultDecoder(), new DefaultDecoder()).get(0)

  // info("setup:complete topic=%s for zk=%s and groupId=%s".format(topic,zookeeperConnect,groupId))

  def read(write: (Array[Byte]) => Unit) = {
    for (messageAndTopic <- stream) {
      try {
        //info("writing from stream")
        write(messageAndTopic.message)
        //info("written to stream")
      } catch {
        case e: Throwable =>
          if (true) { //this is objective even how to conditionalize on it
            error("Error processing message, skipping this message: ", e)
          } else {
            throw e
          }
      }
    }
  }

  def close() {
    connector.shutdown()
  }

  def exec(line: Array[Byte]) = {
  //  queue += new String(line)
    queue.put(new String(line))
  }
  
  def onStart() {
  
    // Start the thread that consume data over Zookeeper. 
    new Thread("kafka Consumer") {
      override def run() {read(exec)}
    }.start()
    
  }
  
}