package com.madhukaraphatak.examples.sparktwo.streaming
import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.apache.spark.sql.streaming.OutputMode

object ProcessingTimeWindow {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("example")
      .getOrCreate()
    //create stream from socket
    sparkSession.sparkContext.setLogLevel("ERROR")
    val socketStreamDf = sparkSession.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 50050)
      .load()
    val currentTimeDf = socketStreamDf.withColumn("processingTime",current_timestamp())
    import sparkSession.implicits._
    val socketDs = currentTimeDf.as[(String, Timestamp)]
    val wordsDs = socketDs
      .flatMap(line => line._1.split(" ").map(word => (word, line._2)))
      .toDF("word", "processingTime")

    val windowedCount = wordsDs
      .groupBy(
        window($"processingTime", "15 seconds")
      )
      .count()
      .orderBy("window")

    val query =
      windowedCount.writeStream
        .format("console")
        .option("truncate","false")
        .outputMode(OutputMode.Complete())

    query.start().awaitTermination()
  }
}
