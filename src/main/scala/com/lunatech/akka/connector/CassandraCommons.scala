package com.lunatech.akka.connector

import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.lunatech.akka.connector.InitializeCassandra._

object CassandraCommons {
  def executeQueryOnSession(query: String): ResultSet = session.execute(query)

  def getRowFromResultSet(resultSet: ResultSet): Row = resultSet.one()

}