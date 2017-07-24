package com.madhukaraphatak.examples.sparktwo.streaming

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

/**
  * Created by madhu on 24/07/17.
  */
object SocketMiniBatchExample {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    //create stream from socket

    val socketStreamDf = sparkSession.readStream.
      format("socket")
      .option("host", "localhost")
      .option("port", 50050).load()

    val query = socketStreamDf.writeStream.format("console").outputMode(OutputMode.Append()).trigger(
      Trigger.ProcessingTime(10, TimeUnit.SECONDS)
    ).start()

    query.awaitTermination()
  }

}
