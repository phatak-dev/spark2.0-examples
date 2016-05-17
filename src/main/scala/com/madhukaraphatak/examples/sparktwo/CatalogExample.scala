package com.madhukaraphatak.examples.sparktwo

import org.apache.spark.sql.SparkSession


/**
  * Catalogue Example
  */
object CatalogExample {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
           master("local")
           .appName("example")
           .getOrCreate()


    val df = sparkSession.read.csv("src/main/resources/sales.csv")
    df.createTempView("sales")

    //interacting with catalogue

    val catalog = sparkSession.catalog

    //print the databases

    catalog.listDatabases().select("name").show()

    // print all the tables

    catalog.listTables().select("name").show()

    // is cached
    println(catalog.isCached("sales"))
    df.cache()
    println(catalog.isCached("sales"))

    // drop the table
    catalog.dropTempView("sales")
    catalog.listTables().select("name").show()

    // list functions
    catalog.listFunctions().select("name","description","className","isTemporary").show(100)
  }

}
