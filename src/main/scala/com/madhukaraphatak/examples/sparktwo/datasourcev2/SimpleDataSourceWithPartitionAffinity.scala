package com.madhukaraphatak.examples.sparktwo.datasourcev2.partitionaffinity

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types._

class DefaultSource extends DataSourceV2 with ReadSupport {

  def createReader(options: DataSourceOptions) = new SimpleDataSourceReader()

}

class SimpleDataSourceReader extends DataSourceReader {

  def readSchema() = StructType(Array(StructField("value", StringType)))

  def planInputPartitions = {
    val factoryList = new java.util.ArrayList[InputPartition[InternalRow]]
    factoryList.add(new SimpleInputPartition(0, 4))
    factoryList.add(new SimpleInputPartition(5, 9))
    factoryList
  }

}

class SimpleInputPartition(var start: Int, var end: Int)
    extends InputPartition[InternalRow] {
  def createPartitionReader = new SimpleInputPartitionReader(start, end)

  override def preferredLocations(): Array[String] = Array("sample-hostname")
}

class SimpleInputPartitionReader(var start: Int, end: Int) extends InputPartitionReader[InternalRow] {

  val values = Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")

  var index = 0

  def next = start <= end

  def get = {
    val row = InternalRow(values(start))
    start = start + 1
    row
  }

  def close() = Unit
}
