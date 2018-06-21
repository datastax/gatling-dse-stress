package com.datastax.gatling.stress.libs

import com.datastax.driver.core.policies._
import com.datastax.driver.core.{ConsistencyLevel, HostDistance, PoolingOptions, QueryOptions}
import com.datastax.driver.dse.auth.DseGSSAPIAuthProvider
import com.datastax.driver.dse.graph.GraphOptions
import com.datastax.driver.dse.{DseCluster, DseSession}
import com.datastax.gatling.stress.config.{CassandraConfiguration, DseStressConfiguration}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.lang3.StringUtils


/**
  * Create Cassandra Session
  */
class Cassandra(conf: Config) extends LazyLogging {


  final val JAVA_SSL_PROP = "javax.net.ssl.trustStore"

  protected val cassandraConf: CassandraConfiguration = DseStressConfiguration(conf).cassandra

  private val clusterBuilder: DseCluster.Builder = initClusterBuilder()

  private var session: DseSession = _
  private var loadBalancingPolicy: LoadBalancingPolicy = getLoadBalancingPolicy


  /**
    * Get Cassandra Session
    *
    * @return
    */
  def getSession: DseSession = {
    if (session == null) {
      createSession
    }
    session
  }


  /**
    * Get Cassandra Cluster
    *
    * @return
    */
  def getCluster: DseCluster = getSession.getCluster


  /**
    * Get Cassandra Cluster Builder
    *
    * @return
    */
  def getBuilder = clusterBuilder


  /**
    * Set Full Cassandra Session vs. auto-building
    *
    * @param dseSession
    */
  def setSession(dseSession: DseSession): Unit = {
    if (session == null) {
      session = dseSession
    }
  }


  /**
    * Get CL from configFile string
    *
    * @param configCL
    * @return
    */
  def getCL(configCL: String): ConsistencyLevel = ConsistencyLevel.valueOf(configCL)


  /**
    * Run an arbitrary query, used for setting up init scripts to create keyspace or tables
    *
    * @param query
    * @return
    */
  def runQuery(query: String) = {
    if (session == null) {
      getSession
    }
    session.execute(query)
  }


  /**
    * Get Load Balance Policy
    *
    * @return
    */
  protected def getLoadBalancingPolicy: LoadBalancingPolicy = {

    if (cassandraConf.dcName.nonEmpty) {
      this.loadBalancingPolicy = new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().withLocalDc(cassandraConf.dcName.get).build())
      logger.debug("dcName found in configs, setting connection to TokenAware(DCAwareRR) w/ dc = {}", cassandraConf.dcName.get)
    } else {
      this.loadBalancingPolicy = new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build())
      logger.debug("dcName empty in configs, setting connection to TokenAware(DCAwareRR) w/ default to first host IP")
    }

    this.loadBalancingPolicy
  }

  /**
    * Create Cassandra Session
    *
    * @return
    */
  protected def createSession: DseSession = {
    session = clusterBuilder.build().connect()
    session
  }


  /**
    * Create Dse Builder with
    *
    * @return
    */
  protected def initClusterBuilder(): DseCluster.Builder = {

    val clusterBuilder = new DseCluster.Builder()

    clusterBuilder.withLoadBalancingPolicy(loadBalancingPolicy)
    clusterBuilder.withPort(cassandraConf.port)

    if (cassandraConf.clusterName.nonEmpty) {
      clusterBuilder.withClusterName(cassandraConf.clusterName.get)
    }

    if (cassandraConf.auth.username.nonEmpty && cassandraConf.auth.password.nonEmpty) {
      logger.debug("Username and password set in configs using to connect to nodes")
      clusterBuilder.withCredentials(cassandraConf.auth.username.get, cassandraConf.auth.password.get)
    }
       else if (cassandraConf.authMethod.nonEmpty) {
      if (cassandraConf.authMethod.get.equalsIgnoreCase("kerberos")) {
        logger.debug("Using kerberos for authentication")
        clusterBuilder.withAuthProvider(DseGSSAPIAuthProvider.builder().build())
      }
    }

    // set default consistency
    if (cassandraConf.defaultConsistency.nonEmpty || cassandraConf.serialConsistency.nonEmpty) {

      val defaultCl: ConsistencyLevel = getCL(cassandraConf.defaultConsistency.getOrElse("LOCAL_ONE"))
      val serialCl: ConsistencyLevel = getCL(cassandraConf.serialConsistency.getOrElse("SERIAL"))

      clusterBuilder.withQueryOptions(new QueryOptions()
          .setConsistencyLevel(defaultCl)
          .setSerialConsistencyLevel(serialCl)
      )

      logger.debug("Using {} as default consistency", defaultCl.toString)
      logger.debug("Using {} as serial consistency", serialCl.toString)
    }

    // set Pooling options
    val poolingOptions = cassandraConf.poolingOptions
    clusterBuilder.withPoolingOptions(
      new PoolingOptions()
          .setConnectionsPerHost(HostDistance.LOCAL, poolingOptions.local.coreConnections,

            if (poolingOptions.local.maxConnections < poolingOptions.local.coreConnections) {
              poolingOptions.local.coreConnections
            } else {
              poolingOptions.local.maxConnections
            }
          )

          .setMaxRequestsPerConnection(HostDistance.LOCAL, cassandraConf.poolingOptions.local.maxRequestsPerConnection)

          .setConnectionsPerHost(HostDistance.REMOTE, cassandraConf.poolingOptions.remote.coreConnections,

            if (poolingOptions.remote.maxConnections < poolingOptions.remote.coreConnections) {
              poolingOptions.remote.coreConnections
            } else {
              poolingOptions.remote.maxConnections
            }

          )

          .setMaxRequestsPerConnection(HostDistance.REMOTE,
            cassandraConf.poolingOptions.remote.maxRequestsPerConnection
          )

          .setMaxQueueSize(cassandraConf.poolingOptions.maxQueueSize)
    )


    // set graph name if present
    if (cassandraConf.graphName.nonEmpty) {
      clusterBuilder.withGraphOptions(new GraphOptions().setGraphName(cassandraConf.graphName.get))
    }

    // add default ssl if found
    if (StringUtils.isNotEmpty(System.getProperty(JAVA_SSL_PROP))) {
      logger.debug("Using SSL trustStore found in passed properties {}", System.getProperty(JAVA_SSL_PROP))
      clusterBuilder.withSSL
    }

    clusterBuilder.addContactPoints(cassandraConf.hosts: _*)

    clusterBuilder
  }

}
