package com.madhukaraphatak.examples.sparktwo.datasourcev2.simplemulti

import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

class DefaultSource extends DataSourceV2 with ReadSupport {

  def createReader(options: DataSourceOptions) = new SimpleDataSourceReader()

}

class SimpleDataSourceReader extends DataSourceReader {

  def readSchema() = StructType(Array(StructField("value", StringType)))

  def planInputPartitions = {
    val factoryList = new java.util.ArrayList[InputPartition[InternalRow]]
    factoryList.add(new SimpleInputPartitionReader(0, 4))
    factoryList.add(new SimpleInputPartitionReader(5, 9))
    factoryList
  }

}

class SimpleInputPartitionReader(var start: Int, var end: Int) extends InputPartition[InternalRow] with
    InputPartitionReader[InternalRow] {

  def createPartitionReader = new SimpleInputPartitionReader(start, end)

  val values = Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")

  var index = 0

  def next = start <= end

  def get = {
    val row = InternalRow(UTF8String.fromString(values(start)))
    start = start + 1
    row
  }

  def close() = Unit
}

