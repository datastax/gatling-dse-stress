package com.datastax.gatling.stress.libs


import java.io.{File, FileWriter}

import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.dse.{DseCluster, DseSession}
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.{Random, Try}

/**
  * Fetch Base Data from C*
  *
  * @param conf
  * @param cass
  */
class FetchBaseData(conf: SimConfig, cass: Cassandra) extends LazyLogging {

  private final val COMMA_CHAR = ","
  private final val PIPE_CHAR = ","

  private final val CONF_DC_NAME = "dcName"
  private final val CONF_KEYSPACE = "keyspace"
  private final val CONF_TABLE = "table"
  private final val CONF_DATA_DIR = "dataDir"
  private final val CONF_DATA_FILE = "dataFile"
  private final val CONF_APPEND_TO_FILE = "appendToFile"
  private final val CONF_TOKEN_RANGES_PER_HOST = "tokenRangesPerHost"
  private final val CONF_PAGINATION_SIZE = "paginationSize"
  private final val CONF_MAX_KEYS = "maxPartitionKeys"
  private final val CONF_PARTITION_KEY_COLUMNS = "partitionKeyColumns"
  private final val CONF_COLUMNS_TO_FETCH = "columnsToFetch"
  private final val CONF_PER_PARTITION_DISABLED = "perPartitionDisabled"

  private val dataDir = conf.getGeneralConfStr(CONF_DATA_DIR)
  private val dcName = conf.getCassandraConfStr(CONF_DC_NAME)

  private val keyspace = conf.getSimulationConfStr(CONF_KEYSPACE)
  private val table = conf.getSimulationConfStr(CONF_TABLE)
  private val partitionKeyColumns = conf.getSimulationConfStrList(CONF_PARTITION_KEY_COLUMNS)
  private val columnsToFetch = conf.getSimulationConfStrList(CONF_COLUMNS_TO_FETCH)
  private val paginationSize = Try(conf.getSimulationConfInt(CONF_PAGINATION_SIZE)).getOrElse(100)
  private val maxPartitionKeys = Try(conf.getSimulationConfInt(CONF_MAX_KEYS)).getOrElse(500)
  private val appendToFile = Try(conf.getSimulationConfBool(CONF_APPEND_TO_FILE)).getOrElse(false)
  private val perPartitionDisabled = Try(conf.getSimulationConfBool(CONF_PER_PARTITION_DISABLED)).getOrElse(false)

  private val dataFilename = conf.getSimulationConfStr(CONF_DATA_FILE)
  private val writefileName: String = dataDir + "/" + dataFilename

  protected val session: DseSession = cass.getSession
  protected val cluster: DseCluster = cass.getSession.getCluster
  protected val metadata: Metadata = cluster.getMetadata
  protected val allHosts: mutable.Set[Host] = metadata.getAllHosts.asScala

  protected var tokenRangesPerHost: Int = Try(conf.getSimulationConfInt(CONF_TOKEN_RANGES_PER_HOST)).getOrElse(10)

  protected val keyspaceTokens: mutable.Map[String, Set[TokenRange]] = mutable.Map[String, Set[TokenRange]]()
  protected val foundTokenRanges: mutable.Set[TokenRange] = mutable.Set[TokenRange]()

  protected val fw: FileWriter = openFileWriter()

  protected var supportsPerPartition: Boolean = false

  logger.debug("Max partition keys per token range: {}. Pagination Size: {}",
    maxPartitionKeys.toString, paginationSize.toString)

  /**
    * Fetch All the Data and Save to a csv file
    */
  def createBaseDataCsv() {

    if (maxPartitionKeys < 1) {
      logger.debug("{} set to {} skipping fetching data...", CONF_MAX_KEYS, maxPartitionKeys)
      return
    }

    try {
      iterateOverRanges(getRanges)
      closeFile()
    } catch {
      case e: Exception =>
        logger.error("Failed to Fetch data. Exception:", e)
        throw new RuntimeException("Exiting.")
    }
  }

  /**
    * Get Ranges for a keyspace
    *
    * @return
    */
  protected def getRanges: Set[TokenRange] = {

    // check if the keyspace token ranges have already been fetched
    if (keyspaceTokens.nonEmpty && keyspaceTokens.get(keyspace).isDefined) {
      logger.trace("Token ranges already fetched for keyspace: {}", keyspace)
      return keyspaceTokens(keyspace)
    }

    logger.debug("Token ranges per host: {}. Total hosts found {}. Max Ranges to query in cluster/dc: {}",
      tokenRangesPerHost.toString, allHosts.size.toString, (tokenRangesPerHost * allHosts.size).toString)

    // get unique token ranges for nodes
    allHosts.foreach(setHostRanges)

    logger.trace("Found Tokens: {}, count {}", foundTokenRanges.toString(), foundTokenRanges.size.toString)

    if (foundTokenRanges.nonEmpty) {
      keyspaceTokens.put(keyspace, foundTokenRanges.toSet)
    }

    foundTokenRanges.toSet
  }


  /**
    *
    * @param host
    */
  protected def setHostRanges(host: Host) {

    // if the DC has been set in the configs and the current host is not in the local DC, skip it...
    if (dcName != null && !dcName.equals(host.getDatacenter)) {
      logger.debug("Node's dcName: {} does not match config dcName: {} Skipping...", host.getDatacenter, dcName)
      return
    }

    if (host.getCassandraVersion.getMajor.equals(3) && host.getCassandraVersion.getMinor >= 6) {
      if (!perPartitionDisabled) {
        supportsPerPartition = true
        logger.debug("Cassandra version is >= 3.6 using PER PARTITION optimization")
      } else {
        logger.debug("Cassandra version is >= 3.6 and supports PER PARTITION optimization, but disabled by config")
      }
    } else {
      logger.debug("Cassandra version is < 3.6 using non-optimized queries")
    }

    // Shuffle list to get random order for token ranges
    val hostTokenRanges = Random.shuffle(metadata.getTokenRanges(keyspace, host).asScala.toSeq)

    // if the host has token range count < requested, set to host token range count
    if (hostTokenRanges.size < tokenRangesPerHost) {
      logger.trace("Node has less tokenRanges: {} than requested: {}. Reverting to host size.",
        hostTokenRanges.size.toString, tokenRangesPerHost.toString)
      tokenRangesPerHost = hostTokenRanges.size
    }

    // if set is empty then it's safe to add this hosts tokens w/o check
    if (foundTokenRanges.isEmpty) {
      for (i <- 0 until tokenRangesPerHost) foundTokenRanges.add(hostTokenRanges(i))
      return
    }

    var hostTokenCnt = 0
    // if the foundTokenRanges isn't empty then more checks are required
    hostTokenRanges.foreach { hostToken =>
      if (hostTokenCnt < tokenRangesPerHost) {
        if (foundTokenRanges.contains(hostToken)) {
          logger.trace("TokenRange {} already in token range list, skipping", hostToken.toString)
        } else {
          foundTokenRanges.add(hostToken)
          hostTokenCnt += 1
        }
      }
    }
  }


  /**
    * for each host write a list of partition keys
    *
    * @param tokenRanges
    * @return boolean
    */
  protected def iterateOverRanges(tokenRanges: Set[TokenRange]) {
    tokenRanges.foreach {
      wrappedTokenRange =>
        wrappedTokenRange.unwrap().asScala.foreach {
          range =>
            Some(getRowColumns(range)).map(writeRowsToFile)
        }
    }
  }


  /**
    * Fetch Rows based on token ranges
    *
    * @param range
    * @return
    */
  protected def getRowColumns(range: TokenRange): Seq[String] = {

    val rangeStart = range.getStart.getValue
    val rangeEnd = range.getEnd.getValue

    val select = QueryBuilder.select(columnsToFetch: _*)
        .from(keyspace, table)
        .where(QueryBuilder.gt(QueryBuilder.token(partitionKeyColumns: _*), rangeStart))
        .and(QueryBuilder.lte(QueryBuilder.token(partitionKeyColumns: _*), rangeEnd))

    if (supportsPerPartition) {
      select.perPartitionLimit(1)
    }

    select.setFetchSize(paginationSize)

    logger.trace("Query being used: {} with fetchSize of {}", select.toString, paginationSize)

    val resultSet: ResultSet = session.execute(select)

    val fetchedRowData = ListBuffer[String]()
    val fetchedPartitionKeys = ArrayBuffer[String]()
    var startingPartitionKey = ""

    resultSet.forEach {
      row =>

        if (fetchedPartitionKeys.size < maxPartitionKeys) {

          val partitionKeyValues = ArrayBuffer[String]()
          partitionKeyColumns.foreach {
            column =>
              partitionKeyValues.append(row.getObject(column).toString)
          }


          if (startingPartitionKey.isEmpty) {
            startingPartitionKey = partitionKeyValues.mkString(COMMA_CHAR)
          }

          val columnValues = ArrayBuffer[String]()
          columnsToFetch.foreach {
            column =>
              columnValues.append(
                if (row.isNull(column)) ""
                else row.getObject(column).toString
              )
          }

          // check if the same pkeys have been fetched already this is to avoid the same pkey in
          // wide rows from being in fetch list multiple times
          val pkeyString = partitionKeyValues.mkString(COMMA_CHAR)
          if (!fetchedPartitionKeys.contains(pkeyString)) {
            fetchedPartitionKeys.append(pkeyString)
            fetchedRowData.append(columnValues.mkString(COMMA_CHAR) + "\n")
          }
        }
    }

    logger.debug("Found {} unique partition key(s) for token range [{} - {}]. First partition key: {}",
      fetchedRowData.size.toString, rangeStart.toString, rangeEnd.toString, startingPartitionKey)

    fetchedPartitionKeys.clear()
    fetchedRowData
  }


  /**
    * Open Data file for writing partition keys
    */
  protected def openFileWriter(): FileWriter = {

    createDataDir()

    logger.debug("Opening data file: {}. Append to file is set to: {}", writefileName, appendToFile.toString)

    val fileWriter: FileWriter = try {
      new FileWriter(writefileName, appendToFile) //the true will append the new data
    } catch {
      case e: Exception =>
        logger.error("Unable to open file to write. Exiting. IOException: {}", e.getMessage)
        throw new RuntimeException()
    }

    if (!appendToFile) {
      fileWriter.write(columnsToFetch.mkString(COMMA_CHAR) + "\n")
    }

    fileWriter
  }


  /**
    * Create the dataDir from configs if it doesn't exist
    *
    * @return
    */
  protected def createDataDir() {
    val theDir = new File(dataDir)
    if (!theDir.exists()) {
      theDir.mkdir()
    }
  }


  /**
    * Close File
    */
  protected def closeFile() {
    logger.debug("Closing data file {}", writefileName)

    try {
      fw.close()
    } catch {
      case e: Exception =>
        logger.error("IOException: {}", e.getMessage)
    }
  }

  /**
    * Write rows fetched to open file
    *
    * @param rows
    */
  protected def writeRowsToFile(rows: Seq[String]): Boolean = {

    if (rows.size < 1) {
      logger.debug("Row count empty, no rows to write.")
      return false
    }

    try {
      rows.foreach(fw.write)
    } catch {
      case e: Exception =>
        logger.error("Unable to write line to file. IOException: {}", e.getMessage)
        throw new RuntimeException()
    }

    true
  }
}
