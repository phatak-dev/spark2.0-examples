package com.madhukaraphatak.examples.sparktwo.datasourcev2.simple

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class DefaultSource extends DataSourceV2 with ReadSupport {

  def createReader(options: DataSourceOptions) = new SimpleDataSourceReader()

}

class SimpleDataSourceReader extends DataSourceReader {

  override def readSchema() = StructType(Array(StructField("value", StringType)))

  override def planInputPartitions = {
    val factoryList = new java.util.ArrayList[InputPartition[InternalRow]]
    factoryList.add(new SimpleInputPartitionReader())
    factoryList
  }

}

class SimpleInputPartitionReader extends InputPartition[InternalRow] with InputPartitionReader[InternalRow] {

  override def createPartitionReader(): InputPartitionReader[InternalRow] = new SimpleInputPartitionReader

  val values = Array("1", "2", "3", "4", "5")

  var index = 0

  def next = index < values.length

  def get = {
    val stringValue = values(index)
    val stringUtf = UTF8String.fromString(stringValue)
    val row = InternalRow(stringUtf)
    index = index + 1
    row
  }

  def close() = Unit


}

