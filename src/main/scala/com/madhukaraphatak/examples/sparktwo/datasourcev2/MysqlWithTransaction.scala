package com.madhukaraphatak.examples.sparktwo.datasourcev2.mysqlwithtransaction

import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources.v2.writer._
import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources._
import java.util.Optional
import org.apache.spark.sql.SaveMode
import java.sql.{Connection, DriverManager}

class DefaultSource extends DataSourceV2 with WriteSupport {

  def createWriter(jobId: String,
                   schema: StructType,
                   mode: SaveMode,
                   options: DataSourceOptions): Optional[DataSourceWriter] = {
    Optional.of(new MysqlDataSourceWriter())

  }
}

class MysqlDataSourceWriter extends DataSourceWriter {

  override def createWriterFactory(): DataWriterFactory[Row] = {
    new MysqlDataWriterFactory()
  }

  override def commit(messages: Array[WriterCommitMessage]) = {}

  override def abort(messages: Array[WriterCommitMessage]) = {
    println("abort is called in  data source writer")
  }

}

class MysqlDataWriterFactory extends DataWriterFactory[Row] {
  override def createDataWriter(partitionId: Int,
                                attemptNumber: Int): DataWriter[Row] = {
    new MysqlDataWriter()
  }
}

class MysqlDataWriter extends DataWriter[Row] {

  val url = "jdbc:mysql://localhost/test"
  val user = "root"
  val password = "abc123"
  val table = "userwrite"

  val connection = DriverManager.getConnection(url, user, password)
  connection.setAutoCommit(false)
  val statement = s"insert into $table (user) values (?)"
  val preparedStatement = connection.prepareStatement(statement)

  def write(record: Row) = {
    val value = record.getString(0)
    preparedStatement.setString(1, value)
    preparedStatement.executeUpdate()
  }

  def commit(): WriterCommitMessage = {
    connection.commit()
    WriteSucceeded
  }

  def abort() = {
    println("abort is called in data writer")
  }

  object WriteSucceeded extends WriterCommitMessage

}
