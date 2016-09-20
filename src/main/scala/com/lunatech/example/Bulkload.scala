package com.lunatech.example

import java.text.SimpleDateFormat
import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import java.io.File
import java.net.HttpURLConnection
import java.net.URL
import java.io.IOException
import java.io.BufferedReader
import java.io.InputStreamReader
import org.supercsv.io.CsvListReader
import org.supercsv.prefs.CsvPreference
import java.text.ParseException
import scala.collection.JavaConverters._
import java.math.BigDecimal
import java.lang.Long

object Bulkload extends App {
  val CSV_URL = "http://real-chart.finance.yahoo.com/table.csv?s=YHOO"

  /** Default output directory */
  val DEFAULT_OUTPUT_DIR = "/trn/eclipse_workspace/scala-cassandra-connect/ruf/"

  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")

  /** Keyspace name */
  val KEYSPACE = "simplex"

  /** Table name */
  val TABLE = "historical_prices"

  /**
   * Schema for bulk loading table.
   * It is important not to forget adding keyspace name before table name,
   * otherwise CQLSSTableWriter throws exception.
   */
  val SCHEMA = String.format("CREATE TABLE %s.%s (" +
    "ticker ascii, " +
    "date timestamp, " +
    "open decimal, " +
    "high decimal, " +
    "low decimal, " +
    "close decimal, " +
    "volume bigint, " +
    "adj_close decimal, " +
    "PRIMARY KEY (ticker, date) " +
    ") WITH CLUSTERING ORDER BY (date DESC)", KEYSPACE, TABLE)

  /**
   * INSERT statement to bulk load.
   * It is like prepared statement. You fill in place holder for each data.
   */
  val INSERT_STMT = String.format("INSERT INTO %s.%s (" +
    "ticker, date, open, high, low, close, volume, adj_close" +
    ") VALUES (" +
    "?, ?, ?, ?, ?, ?, ?, ?" +
    ")", KEYSPACE, TABLE)

  // magic!
  Config.setClientMode(true)

  // Create output directory that has keyspace and table name in the path
  val outputDir = new File(DEFAULT_OUTPUT_DIR + File.separator + KEYSPACE + File.separator + TABLE)
  if (!outputDir.exists() && !outputDir.mkdirs()) {
    throw new RuntimeException("Cannot create output directory: " + outputDir)
  }

  // Prepare SSTable writer
  val builder: CQLSSTableWriter.Builder = CQLSSTableWriter.builder()
  // set output directory
  builder.inDirectory(outputDir)
    // set target schema
    .forTable(SCHEMA)
    // set CQL statement to put data
    .using(INSERT_STMT)
    // set partitioner if needed
    // default is Murmur3Partitioner so set if you use different one.
    .withPartitioner(new Murmur3Partitioner())

  val writer: CQLSSTableWriter = builder.build()

  var conn: HttpURLConnection = null
  try {
    val url = new URL(String.format(CSV_URL, "YHOO"))
    conn = url.openConnection().asInstanceOf[HttpURLConnection]
  } catch {
    case e: IOException => throw new RuntimeException(e)
  }

  try {
    val reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))
    val csvReader = new CsvListReader(reader, CsvPreference.STANDARD_PREFERENCE)

    csvReader.getHeader(true)

    var line = csvReader.read() //.asScala.toList

    while (line != null) {

      // We use Java types here based on
      // http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/DataType.Name.html#asJavaClass%28%29
      writer.addRow("YHOO",
        DATE_FORMAT.parse(line.get(0)),
        new BigDecimal(line.get(1)),
        new BigDecimal(line.get(2)),
        new BigDecimal(line.get(3)),
        new BigDecimal(line.get(4)),
        new Long(Long.parseLong(line.get(5))),
        new BigDecimal(line.get(6)))
      line = csvReader.read()
    }
  } catch {
    case e: InvalidRequestException => e.printStackTrace()
    case e: ParseException          => e.printStackTrace()
    case e: IOException             => e.printStackTrace()

  }

  //close
  writer.close()

}