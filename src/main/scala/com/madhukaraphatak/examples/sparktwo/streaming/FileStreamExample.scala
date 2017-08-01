package com.madhukaraphatak.examples.sparktwo.streaming

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by madhu on 24/07/17.
  */
object FileStreamExample {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("example")
      .getOrCreate()

    val schema = StructType(
      Array(StructField("transactionId", StringType),
            StructField("customerId", StringType),
            StructField("itemId", StringType),
            StructField("amountPaid", StringType)))

    //create stream from folder
    val fileStreamDf = sparkSession.readStream
      .option("header", "true")
      .schema(schema)
      .csv("/tmp/input")

    val query = fileStreamDf.writeStream
      .format("console")
      .outputMode(OutputMode.Append()).start()

      query.awaitTermination()

  }

}
