package com.lunatech.akka.connector

import com.datastax.driver.core.ResultSet
import com.lunatech.akka.connector.CassandraCommons._
import scala.collection.JavaConverters._
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import java.io.File
import akka.stream.impl.io.InputStreamSource
import akka.stream.scaladsl.Source
import akka.{ NotUsed, Done }
import com.datastax.driver.core.Row
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Flow
import java.io.PrintStream
import java.io.FileOutputStream
import scala.concurrent.Future
import akka.stream.scaladsl.Keep
import scala.util.Success
import scala.util.Failure
import akka.stream.scaladsl.FileIO
import akka.util.ByteString

object StreamApp extends App {

  /**
   * query for retrieving the data
   */

  val out = new PrintStream(new FileOutputStream("/trn/eclipse_workspace/scala-cassandra-connect/logs/log1"))
  System.setOut(out)

  val schemaQuery = "SELECT * FROM simplex.historical_prices;" //pre-existing data with keyspace as simplex and table name as historical_proces

  val rs1: ResultSet = executeQueryOnSession(schemaQuery)
  val all1 = rs1.asScala.seq.to[collection.immutable.Iterable] // convert iterable to immutable iterable

  /**
   * implicit parameters for akka streams
   */
  implicit val system = ActorSystem("cassandra-stream-hbase")
  implicit val materializer = ActorMaterializer()
  import system.dispatcher

  //defining source for streams
  val source: Source[Row, NotUsed] = Source(all1)
  
  //def getRowx(r1: Row): Option[Rowx] = Some(Rowx(r1.toString()))

  //val flow1 = Flow[Row].mapAsyncUnordered(8) { x => Future(getRowx(x)) }.collect{case Some(v) => v}
  val flow1 = Flow[Row].mapAsyncUnordered(8) { x => Future(Some(x)) }.collect{case Some(v) => v} // here we can do the transformations, in this case i have just kept it as it is
  // but we can parse the object , do the transformations
  
  
  val fileSink = FileIO.toFile(new File("/trn/eclipse_workspace/scala-cassandra-connect/target/cassandra-export.txt"))

  //val sink = Sink.foreach[Rowx] { x => println(s"The row recieved at sink is => $x") }

  val sink = Flow[Row].map (i =>  ByteString(i.toString + '\n') ).toMat(fileSink)((_, bytesWritten) =>  bytesWritten)

  
  val runnab = source.via(flow1).toMat( sink)(Keep.right)

  val k = runnab.run().onComplete { x =>
      x match {
        case Success((t)) => println("something complete here")
        case Failure(tr) => println("Something went wrong => "+tr)
      }
      system.shutdown()
    }

  

  /**
   * finally close all the variables
   */
  InitializeCassandra.closeAll

}