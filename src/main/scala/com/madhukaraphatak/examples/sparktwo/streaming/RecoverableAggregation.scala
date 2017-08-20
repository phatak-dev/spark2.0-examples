package com.madhukaraphatak.examples.sparktwo.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType
}

object RecoverableAggregation {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("example")
      .getOrCreate()

    val schema = StructType(
      Array(StructField("transactionId", StringType),
            StructField("customerId", StringType),
            StructField("itemId", StringType),
            StructField("amountPaid", DoubleType)))

    //create stream from folder
    val fileStreamDf = sparkSession.readStream
      .option("header", "true")
      .schema(schema)
      .csv("/tmp/input")

    val countDs = fileStreamDf.groupBy("customerId").sum("amountPaid")
    val query =
      countDs.writeStream
        .format("console")
        .option("checkpointLocation", "/tmp/checkpoint")
        .outputMode(OutputMode.Complete())

    query.start().awaitTermination()
  }
}
