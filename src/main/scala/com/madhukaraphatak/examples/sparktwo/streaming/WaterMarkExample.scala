package com.madhukaraphatak.examples.sparktwo.streaming

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object WaterMarkExample {

  case class Stock(time: Timestamp, symbol: String, value: Double)

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("example")
      .getOrCreate()
    //create stream from socket

    import sparkSession.implicits._
    sparkSession.sparkContext.setLogLevel("ERROR")
    val socketStreamDs = sparkSession.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 50050)
      .load()
      .as[String]

    // read as stock
    val stockDs = socketStreamDs.map(value => {
      val columns = value.split(",")
      Stock(new Timestamp(columns(0).toLong), columns(1), columns(2).toDouble)
    })

    val windowedCount = stockDs
      .withWatermark("time", "500 milliseconds")
      .groupBy(
        window($"time", "10 seconds")
      )
      .sum("value")

    val query =
      windowedCount.writeStream
        .format("console")
        .option("truncate", "false")
        .outputMode(OutputMode.Update())

    query.start().awaitTermination()
  }
}
