name := "scala-cassandra-connect"
 
version := "1.0"
 
scalaVersion := "2.11.8"
 
sbtVersion := "0.13.12"

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.0"
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-mapping" % "3.1.0"
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-extras" % "3.1.0"
libraryDependencies += "org.apache.cassandra" % "cassandra-all" % "2.1.12"
libraryDependencies += "net.sf.supercsv" % "super-csv" % "2.1.0"

