package com.madhukaraphatak.examples.sparktwo.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

/**
 * Created by madhu on 24/07/17.
 */
object StreamJoin {

  case class Sales(
    transactionId: String,
    customerId:    String,
    itemId:        String,
    amountPaid:    Double)
  case class Customer(customerId: String, customerName: String)
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
    //take customer data as static df
    val customerDs = sparkSession.read
      .format("csv")
      .option("header", true)
      .load("src/main/resources/customers.csv")
      .as[Customer]

    import sparkSession.implicits._
    val dataDf = socketStreamDf.as[String].flatMap(value ⇒ value.split(" "))
    val salesDs = dataDf
      .as[String]
      .map(value ⇒ {
        val values = value.split(",")
        Sales(values(0), values(1), values(2), values(3).toDouble)
      })

    val joinedDs = salesDs
      .join(customerDs, "customerId")
    //create sales schema
    val query =
      joinedDs.writeStream.format("console").outputMode(OutputMode.Append())

    query.start().awaitTermination()
  }
}
