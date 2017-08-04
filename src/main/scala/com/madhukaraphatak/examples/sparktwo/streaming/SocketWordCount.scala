package com.madhukaraphatak.examples.sparktwo.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

/**
  * Created by madhu on 24/07/17.
  */
object SocketWordCount {

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

    import sparkSession.implicits._
    val socketDs = socketStreamDf.as[String]
    val wordsDs =  socketDs.flatMap(value => value.split(" "))
    val countDs = wordsDs.groupBy("value").count()

    val query =
      countDs.writeStream.format("console").outputMode(OutputMode.Complete())

    query.start().awaitTermination()
  }
}
