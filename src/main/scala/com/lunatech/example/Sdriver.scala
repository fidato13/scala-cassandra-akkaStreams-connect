package com.lunatech.example

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.datastax.driver.core.Session
import com.lunatech.example.CassandraCommons_poc._
import scala.collection.JavaConverters._

object Sdriver extends App {

  val releaseVersionQuery = "select release_version from system.local"
  val createKeySpaceQuery = "CREATE KEYSPACE IF NOT EXISTS simplex WITH replication " +
    "= {'class':'SimpleStrategy', 'replication_factor':1};"
  val createTableQuery = "CREATE TABLE IF NOT EXISTS simplex.playlists (" +
    "id uuid," +
    "title text," +
    "album text, " +
    "artist text," +
    "song_id uuid," +
    "PRIMARY KEY (id, title, album, artist)" +
    ");"
    
   val createBulkLoadTableQuery = "CREATE TABLE simplex.historical_prices (" +
    "ticker ascii, " +
    "date timestamp, " +
    "open decimal, " +
    "high decimal, " +
    "low decimal, " +
    "close decimal, " +
    "volume bigint, " +
    "adj_close decimal, " +
    "PRIMARY KEY (ticker, date) " +
    ") WITH CLUSTERING ORDER BY (date DESC)"
    
    
  val insertDataQuery = "INSERT INTO simplex.playlists (id, song_id, title, album, artist) " +
    "VALUES (" +
    "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
    "756716f7-2e54-4715-9f00-91dcbea6cf50," +
    "'La Petite Tonkinoise'," +
    "'Bye Bye Blackbird'," +
    "'Joséphine Baker'" +
    ");"

  val schemaQuery = "SELECT * FROM simplex.playlists " +
    "WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;"

  /**
   * Custom function  for retrieving the release version of cassandra
   */
  def custom_GetCassandraReleaseVersion: String = {
    val resultSetQuery: ResultSet = executeQueryOnSession(releaseVersionQuery)
    val rowQuery: Row = getRowFromResultSet(resultSetQuery)
    val releaseVersion: String = rowQuery.getString("release_version")

    releaseVersion

    //Sample Usage - println("Release version => " + custom_GetCassandraReleaseVersion)

  }

  def createSchema = {
    // execute above create keyspace query
    //executeQueryOnSession(createKeySpaceQuery)

    // execute above create table query
    executeQueryOnSession(createBulkLoadTableQuery)

  }

  def loadData = {

    executeQueryOnSession(insertDataQuery)
  }

  def querySchema = {

    val rs: ResultSet = executeQueryOnSession(schemaQuery)
    val allRow = rs.all().asScala.toList.foreach { row =>
      printf("The TTTTTTTTTTTTTTTTTTTTTTT %-30s\t%-20s\t%-20s%n",
        row.getString("title"),
        row.getString("album"),
        row.getString("artist"))
    }

  }

  printf("Connected to cluster: %s%n", InitializeCassandra_poc.cluster.getMetadata().getClusterName())

  //  createSchema
  // loadData
   querySchema
  
  InitializeCassandra_poc.closeAll

}