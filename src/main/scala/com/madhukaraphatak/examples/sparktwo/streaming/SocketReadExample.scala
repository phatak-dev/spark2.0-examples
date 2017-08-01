package com.madhukaraphatak.examples.sparktwo.streaming

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by madhu on 24/07/17.
  */
object SocketReadExample {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("example")
      .getOrCreate()

    //create stream from socket

    val socketStreamDf = sparkSession.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 50050)
      .load()

    val consoleDataFrameWriter = socketStreamDf.writeStream
      .format("console")
      .outputMode(OutputMode.Append())

    val query = consoleDataFrameWriter.start()

    query.awaitTermination()

  }

}
