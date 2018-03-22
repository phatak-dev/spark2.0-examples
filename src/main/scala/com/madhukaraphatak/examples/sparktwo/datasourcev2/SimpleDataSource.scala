package com.madhukaraphatak.examples.sparktwo.datasourcev2.simple

import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources.v2.reader._
import scala.collection.JavaConverters._

class DefaultSource extends DataSourceV2 with ReadSupport {

  def createReader(options: DataSourceOptions) = new SimpleDataSourceReader()

}

class SimpleDataSourceReader extends DataSourceReader {

  def readSchema() = StructType(Array(StructField("value", StringType)))

  def createDataReaderFactories = {
    val factoryList = new java.util.ArrayList[DataReaderFactory[Row]]
    factoryList.add(new SimpleDataSourceReaderFactory())
    factoryList
  }

}

class SimpleDataSourceReaderFactory extends DataReaderFactory[Row] with DataReader[Row] {
  def createDataReader = new SimpleDataSourceReaderFactory()

  val values = Array("1", "2", "3", "4", "5")

  var index = 0

  def next = index < values.length

  def get = {
    val row = Row(values(index))
    index = index + 1
    row
  }

  def close() = Unit
}

