package com.madhukaraphatak.examples.sparktwo.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{ GroupState, GroupStateTimeout }

import scala.util.Try

case class Session(sessionId: String, value: Double, endSignal: Option[String])

case class SessionInfo(
  totalSum: Double)

case class SessionUpdate(
  id:       String,
  totalSum: Double,
  expired:  Boolean)

object SessionisationExample {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("example")
      .getOrCreate()
    //create stream from socket
    sparkSession.sparkContext.setLogLevel("ERROR")
    val socketStreamDf = sparkSession.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 50050)
      .load()
    import sparkSession.implicits._
    val socketDs = socketStreamDf.as[String]

    // events
    val events = socketDs.map(line ⇒ {
      val columns = line.split(",")
      val endSignal = Try(Some(columns(2))).getOrElse(None)
      Session(columns(0), columns(1).toDouble, endSignal)
    })

    val sessionUpdates = events.groupByKey(_.sessionId)
      .mapGroupsWithState[SessionInfo, SessionUpdate](GroupStateTimeout.NoTimeout()) {
        case (sessionId: String, eventsIter: Iterator[Session], state: GroupState[SessionInfo]) ⇒
          val events = eventsIter.toSeq
          val updatedSession = if (state.exists) {
            val existingState = state.get
            val updatedEvents = SessionInfo(existingState.totalSum + events.map(event ⇒ event.value).reduce(_ + _))
            updatedEvents
          }
          else {
            SessionInfo(events.map(event => event.value).reduce(_+_))
          }
          state.update(updatedSession)
          //check did we get end signal or not
          val isEndSignal = events.filter(value ⇒ value.endSignal.isDefined).length > 0
          if (isEndSignal) {
            state.remove()
            SessionUpdate(sessionId, updatedSession.totalSum, true)
          }
          else {
            SessionUpdate(sessionId, updatedSession.totalSum, false)
          }
      }

    val query = sessionUpdates
      .writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()

  }
}
