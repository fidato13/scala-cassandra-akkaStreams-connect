package com.lunatech.example

import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.lunatech.example.InitializeCassandra_poc._

object CassandraCommons_poc {
  def executeQueryOnSession(query: String): ResultSet = session.execute(query)

  def getRowFromResultSet(resultSet: ResultSet): Row = resultSet.one()

}