package com.lunatech.example

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.datastax.driver.core.Session

object InitializeCassandra_poc {
  val CONTACT_POINTS: String = "127.0.0.1"
  val PORT = 9042

  val cluster: Cluster = Cluster.builder.addContactPoints(CONTACT_POINTS).withPort(PORT).build

  // The Session is what you use to execute queries. Likewise, it is thread-safe and should be reused.
  val session: Session = cluster.connect()
  
  def closeAll = {
    cluster.close
    session.close
  }

}