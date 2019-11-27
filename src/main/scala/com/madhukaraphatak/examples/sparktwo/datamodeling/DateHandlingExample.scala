package com.madhukaraphatak.examples.sparktwo.datamodeling

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object DateHandlingExample {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    import sparkSession.implicits._

    val dataPath = "src/main/resources/datamodeling/date_dim.csv"

    val originalDf = sparkSession.read.format("csv").
      option("header","true")
      .option("inferSchema","true")
      .load(dataPath)

    //replace space in the column names
    val new_columns = originalDf.schema.fields
      .map(value => value.copy(name = value.name.replaceAll("\\s+","_")))

    val newSchema = StructType(new_columns)
    val newNameDf = sparkSession.createDataFrame(originalDf.rdd, newSchema)


    import org.apache.spark.sql.functions._
    val dateDf = newNameDf.withColumn("full_date_formatted",
      to_date(newNameDf.col("full_date"),"dd/MM/yy"))
    dateDf.printSchema()

    //apple stock
    val appleStockDf = sparkSession.read.format("csv").
      option("header","true")
      .option("inferSchema","true")
      .load("src/main/resources/applestock_2013.csv")

    appleStockDf.show()
    appleStockDf.createOrReplaceTempView("stocks")

    // spark date function analysis

    // find is there any records on weekend
    assert(sparkSession.sql("select * from stocks where dayofweek(Date)==1 or dayofweek(Date)==7").count() == 0)

    appleStockDf.groupBy(year($"Date"),quarter($"Date")).
      avg("Close").
      sort("year(Date)","quarter(Date)")
      .show()

    //join apple stock with date and see does it preseved

    val joinedDF = appleStockDf.join(dateDf, appleStockDf.col("Date") ===
      dateDf.col("full_date_formatted"))

    //kind of questions we can ask

    //there is no stock price for weekends
    assert(joinedDF.filter("weekday_flag != 'y'").count()==0)
    //get quaterly max price
    joinedDF.groupBy("year","quarter").
      avg("Close").
      sort("year","quarter")
      .show()



  }
}
