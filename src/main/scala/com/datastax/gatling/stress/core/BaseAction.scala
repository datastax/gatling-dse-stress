package com.datastax.gatling.stress.core


import com.datastax.driver.core.ResultSet
import com.datastax.driver.dse.DseSession
import com.datastax.gatling.stress.libs.{Cassandra, SimConfig}
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


abstract class BaseAction(var cass: Cassandra, var appConf: SimConfig) extends LazyLogging {

  private final val CREATE_KEYSPACE_PATH = "createKeyspace"
  private final val CREATE_KEYSPACE_TOPOLOGY_PATH = "topology"
  private final val CREATE_KEYSPACE_REPLICATION_PATH = "replication"

  private final val STRATEGY_NETWORK = "NetworkTopologyStrategy"
  private final val STRATEGY_SIMPLE = "SimpleStrategy"

  protected lazy val keyspace: String = appConf.getSimulationConfStr("keyspace")
  protected lazy val table: String = appConf.getSimulationConfStr("table")

  protected lazy val session: DseSession = cass.getSession

  protected object Groups {
    val SELECT = "Select"
    val INSERT = "Insert"
    val UPDATE = "Update"
    val DELETE = "Delete"
  }


  /**
    * Runs Array of Queries against current C* Session
    *
    * @param queries
    */
  protected def runQueries(queries: Array[String]): Unit = {
    runQueries(queries.toSeq)
  }

  /**
    * Run Seq of Queries against current C* Session
    *
    * @param queries
    */
  protected def runQueries(queries: Seq[String]): Unit = {
    for (query <- queries) {
      executeQuery(query)
      Thread sleep 100
    }
  }


  /**
    *
    * {{{
    * createKeyspace {
    *   topology = NetworkToplogyStrategy
    *   replication {
    *     dc1: 3
    *     dc2: 3
    *    }
    * }
    * }}}
    *
    * @return
    */
  def createKeyspace: Boolean = {

    if (!appConf.getConf.hasPath(s"simulations.${appConf.getSimName}.$CREATE_KEYSPACE_PATH")) {
      logger.error("Section {} was not found in simulation: {} section", CREATE_KEYSPACE_PATH, appConf.getSimName)
      throw new RuntimeException("Invalid configuration file")
    }

    val keyspaceCreateConf = appConf.getConf.getConfig(s"simulations.${appConf.getSimName}.$CREATE_KEYSPACE_PATH")
    val topology: String = keyspaceCreateConf.getString(CREATE_KEYSPACE_TOPOLOGY_PATH)

    if (keyspaceCreateConf.hasPath("enabled") && !keyspaceCreateConf.getBoolean("enabled")) {
      logger.debug("createKeyspace section found in simulation {}, but enabled is false, skipping...", appConf.getSimName)
      return false
    }

    var query: String = ""

    topology.toLowerCase match {
      case ("networktopology" | "networktopologystrategy" | "network") =>
        val repDcs = keyspaceCreateConf.getObject(CREATE_KEYSPACE_REPLICATION_PATH).unwrapped().asScala
        val dcRepArray = new ArrayBuffer[(String, Int)]()
        for ((k, v) <- repDcs) {
          dcRepArray.append((k, Integer.parseInt(v.toString)))
        }
        query = buildCreateNetworkTopologyKeyspaceQuery(keyspace, dcRepArray.toArray)

      case ("simplestrategy" | "simple") =>
        val rep = keyspaceCreateConf.getInt(CREATE_KEYSPACE_REPLICATION_PATH)
        query = buildCreateSimpleKeyspaceQuery(keyspace, rep)

      case _ =>
        logger.error("Invalid topology for keyspace given, please check configs")
        throw new RuntimeException("Invalid configuration file")
    }

    logger.debug("Creating keyspace {} if not existing using topology {}", keyspace, topology)
    logger.trace("Create keyspace query: {}", query)

    executeQuery(query).wasApplied()
  }

  /**
    * Execute Query with no delay
    *
    * @param query
    */
  protected def executeQuery(query: String): ResultSet = {
    session.execute(query)
  }

  protected def buildCreateSimpleKeyspaceQuery(keyspaceName: String, repFactor: Int): String = {
    this.buildCreateKeyspaceQuery(STRATEGY_SIMPLE, keyspaceName, repFactor)
  }

  protected def buildCreateNetworkTopologyKeyspaceQuery(keyspaceName: String, dcRep: Array[(String, Int)]): String = {
    this.buildCreateKeyspaceQuery(STRATEGY_NETWORK, keyspaceName, 0, dcRep)
  }

  /**
    * Builds query to send to the cluster:
    *
    * CREATE KEYSPACE "Excalibur"
    * WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'dc1' : 3, 'dc2' : 2};
    *
    * CREATE KEYSPACE Excelsior
    * WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
    *
    * @param strategyType
    * @param keyspaceName
    * @param repFactor
    * @param dcRep
    * @return
    */
  private def buildCreateKeyspaceQuery(strategyType: String, keyspaceName: String, repFactor: Int,
                                       dcRep: Array[(String, Int)] = null): String = {

    val query = StringBuilder.newBuilder
    query.append("CREATE KEYSPACE IF NOT EXISTS")
    query.append(s""" "$keyspaceName"""")
    query.append(" WITH REPLICATION = ")

    strategyType match {

      case STRATEGY_SIMPLE =>
        query.append(s"{ 'class': 'SimpleStrategy', 'replication_factor' : $repFactor }")

      case STRATEGY_NETWORK =>
        query.append("{ 'class': 'NetworkTopologyStrategy', ")

        val dcArr = new ArrayBuffer[String]()
        dcRep.foreach(i => {
          dcArr.append(s"'${i._1}': ${i._2}")
        })
        query.append(dcArr.mkString(", "))
        query.append(" }")
    }

    query.append(";")
    query.toString()
  }

}