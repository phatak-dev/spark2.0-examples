package com.madhukaraphatak.examples.sparktwo.datasourcev2

import org.apache.spark.Partition
import org.apache.spark.sql.SparkSession

object DataSourceV2Example {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder
      .master("local[2]")
      .appName("example")
      .getOrCreate()

    val simpleDf = sparkSession.read
      .format("com.madhukaraphatak.examples.sparktwo.datasourcev2.simple")
      .load()

    simpleDf.show()
    println(
      "number of partitions in simple source is " + simpleDf.rdd.getNumPartitions)

    val simpleMultiDf = sparkSession.read
      .format("com.madhukaraphatak.examples.sparktwo.datasourcev2.simplemulti")
      .load()

    simpleMultiDf.show()
    println(
      "number of partitions in simple multi source is " + simpleMultiDf.rdd.getNumPartitions)

    val simpleCsvDf = sparkSession.read
      .format("com.madhukaraphatak.examples.sparktwo.datasourcev2.simplecsv")
      .load("src/main/resources/sales.csv")

    simpleCsvDf.printSchema()
    simpleCsvDf.show()
    println(
      "number of partitions in simple csv source is " + simpleCsvDf.rdd.getNumPartitions)

    val simpleMysqlDf = sparkSession.read
      .format("com.madhukaraphatak.examples.sparktwo.datasourcev2.simplemysql")
      .load()

    simpleMysqlDf.printSchema()
    simpleMysqlDf.filter("user=\"root\"").show()
    println(
      "number of partitions in simple mysql source is " + simpleMysqlDf.rdd.getNumPartitions)

    //write examples
    simpleMysqlDf.write
      .format(
        "com.madhukaraphatak.examples.sparktwo.datasourcev2.simplemysqlwriter")
      .save()
    simpleMysqlDf.write
      .format(
        "com.madhukaraphatak.examples.sparktwo.datasourcev2.mysqlwithtransaction")
      .save()

    val simplePartitoningDf = sparkSession.read
      .format(
        "com.madhukaraphatak.examples.sparktwo.datasourcev2.partitionaffinity")
      .load()

    val dfRDD = simplePartitoningDf.rdd
    val baseRDD =
      dfRDD.dependencies.head.rdd.dependencies.head.rdd.dependencies.head.rdd

    val partition = baseRDD.partitions(0)
    val getPrefferedLocationDef = baseRDD.getClass
      .getMethod("getPreferredLocations", classOf[Partition])
    val preferredLocation = getPrefferedLocationDef
      .invoke(baseRDD, partition)
      .asInstanceOf[Seq[String]]
    println("preferred location is " + preferredLocation)

    sparkSession.stop()

  }
}
