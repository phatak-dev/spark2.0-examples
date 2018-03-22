package com.madhukaraphatak.examples.sparktwo.datasourcev2.simplecsv

import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources.v2.reader._
import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession

class DefaultSource extends DataSourceV2 with ReadSupport {

  def createReader(options: DataSourceOptions) = {
    val path = options.get("path").get
    new SimpleCsvDataSourceReader(path)
  }
}

class SimpleCsvDataSourceReader(path: String) extends DataSourceReader {

  def readSchema() = {
    val sparkContext = SparkSession.builder.getOrCreate().sparkContext
    val firstLine = sparkContext.textFile(path).first()
    val columnNames = firstLine.split(",")
    val structFields = columnNames.map(value ⇒ StructField(value, StringType))
    StructType(structFields)
  }

  def createDataReaderFactories = {
    val sparkContext = SparkSession.builder.getOrCreate().sparkContext
    val rdd = sparkContext.textFile(path)

    val factoryList = new java.util.ArrayList[DataReaderFactory[Row]]
    (0 to rdd.getNumPartitions - 1).foreach(value ⇒
      factoryList.add(new SimpleCsvDataSourceReaderFactory(value, path)))
    factoryList
  }

}

class SimpleCsvDataSourceReaderFactory(partitionNumber: Int, filePath: String, hasHeader: Boolean = true) extends DataReaderFactory[Row] {

  def createDataReader = new SimpleCsvDataReader(partitionNumber, filePath, hasHeader)
}

class SimpleCsvDataReader(partitionNumber: Int, filePath: String, hasHeader: Boolean = true) extends DataReader[Row] {

  var iterator: Iterator[String] = null

  @transient
  def next = {
    if (iterator == null) {
      val sparkContext = SparkSession.builder.getOrCreate().sparkContext
      val rdd = sparkContext.textFile(filePath)
      val filterRDD = if (hasHeader) {
        val firstLine = rdd.first
        rdd.filter(_ != firstLine)
      }
      else rdd
      val partition = filterRDD.partitions(partitionNumber)
      iterator = filterRDD.iterator(partition, org.apache.spark.TaskContext.get())
    }
    iterator.hasNext
  }

  def get = {
    println("calling get")
    val line = iterator.next()
    Row.fromSeq(line.split(","))
  }
  def close() = Unit
}

