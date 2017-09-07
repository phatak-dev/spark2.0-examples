package com.madhukaraphatak.examples.sparktwo.streaming

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object IngestionTimeWindow {

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
      .option("includeTimestamp", true)
      .load()
    import sparkSession.implicits._
    val socketDs = socketStreamDf.as[(String, Timestamp)]
    val wordsDs = socketDs
      .flatMap(line => line._1.split(" ").map(word => {
         Thread.sleep(15000)
        (word, line._2)
      }))
      .toDF("word", "timestamp")

    val windowedCount = wordsDs
      .groupBy(
        window($"timestamp", "15 seconds")
      )
        .count()
      .orderBy("window")


    val query =
      windowedCount.writeStream
        .format("console").option("truncate","false")
        .outputMode(OutputMode.Complete()).start()


    query.awaitTermination()
  }
}
