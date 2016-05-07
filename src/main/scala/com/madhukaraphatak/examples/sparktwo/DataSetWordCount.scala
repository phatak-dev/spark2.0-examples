package com.madhukaraphatak.examples.sparktwo

import org.apache.spark.sql.SparkSession

/**
  * Created by madhu on 6/5/16.
  */
object DataSetWordCount {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    import sparkSession.implicits._
    val data = sparkSession.read.text("src/main/resources/data.txt").as[String]

    val words = data.flatMap(value => value.split("\\s+"))

    val groupedWords = words.groupByKey(_.toLowerCase)

    val counts = groupedWords.count()

    counts.show()


  }

}
