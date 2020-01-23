package com.datastax.gatling.stress.config

import java.net.InetAddress

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration

object DseStressConfiguration extends LazyLogging {

  protected val defaultConf: Config = ConfigFactory.load("default.conf")
  protected val gatlingConf: Config = ConfigFactory.load("gatling.conf")

  def apply(config: Config) = {

    DseStressConfig(

      general = GeneralConfiguration(
        dataDir = config.getString(DseStressConfKeys.general.dataDir).trim
      ),

      cassandra = CassandraConfiguration(
        hosts = getStringListOrString(config, DseStressConfKeys.cassandra.hosts),
        port = config.getInt(DseStressConfKeys.cassandra.port),
        dcName = getStringOrNone(config, DseStressConfKeys.cassandra.dcName),
        clusterName = getStringOrNone(config, DseStressConfKeys.cassandra.clusterName),
        defaultConsistency = getStringOrNone(config, DseStressConfKeys.cassandra.defaultConsistency),
        serialConsistency = getStringOrNone(config, DseStressConfKeys.cassandra.serialConsistency),
        defaultKeyspace = getStringOrNone(config, DseStressConfKeys.cassandra.defaultKeyspace),
        authMethod = getStringOrNone(config, DseStressConfKeys.cassandra.authMethod),

        auth = CassandraAuthConfiguration(
          username = getStringOrNone(config, DseStressConfKeys.cassandra.auth.username),
          password = getStringOrNone(config, DseStressConfKeys.cassandra.auth.password)
        ),

        poolingOptions = CassandraPoolingConfiguration(
          local = CassandraPoolingConnections(
            coreConnections = config.getInt(DseStressConfKeys.cassandra.poolingOptions.local.coreConnections),
            maxConnections = config.getInt(DseStressConfKeys.cassandra.poolingOptions.local.maxConnections),
            maxRequestsPerConnection = config.getInt(DseStressConfKeys.cassandra.poolingOptions.local.maxRequests)
          ),
          remote = CassandraPoolingConnections(
            coreConnections = config.getInt(DseStressConfKeys.cassandra.poolingOptions.remote.coreConnections),
            maxConnections = config.getInt(DseStressConfKeys.cassandra.poolingOptions.remote.maxConnections),
            maxRequestsPerConnection = config.getInt(DseStressConfKeys.cassandra.poolingOptions.remote.maxRequestsPerConnection)
          ),

          maxQueueSize = config.getInt(DseStressConfKeys.cassandra.poolingOptions.maxQueueSize)
        ),

        graphName = getStringOrNone(config, DseStressConfKeys.cassandra.graphName),

        graphiteConf = GraphiteConfiguration(
          host = getStringOrNone(config, DseStressConfKeys.cassandra.graphite.host),
          port = config.getInt(DseStressConfKeys.cassandra.graphite.port),
          prefix = parseGraphitePrefix(config.getString(DseStressConfKeys.cassandra.graphite.prefix)),
          interval = Duration(config.getString(DseStressConfKeys.cassandra.graphite.interval))
        )
      ),

      config = config
    )
  }

  private def getStringOrNone(config: Config, path: String): Option[String] = {
    if (!config.hasPath(path) || config.getIsNull(path)) {
      None
    } else {
      if (config.getString(path).trim.isEmpty) {
        None
      } else {
        Some(config.getString(path).trim)
      }
    }
  }

  private def getStringListOrString(config: Config, path: String): List[String] = {
    var hostList = List[String]()
    try {
      hostList = config.getStringList(path).asScala.toList
    } catch {
      case _: Exception =>
        try {
          hostList = config.getString(path).split(",").toList
        } catch {
          case e: Exception =>
            logger.error(s"Unable to load hosts from config file, please check configs. Exiting...", e)
            throw new RuntimeException(s"Unable to load hosts from config file, please check configs. Exiting...")
        }
    }
    hostList
  }

  private def parseGraphitePrefix(prefix: String): String =
    prefix.replace("${hostname}", InetAddress.getLocalHost.getHostName)

}

case class GeneralConfiguration(dataDir: String)

case class GraphiteConfiguration(host: Option[String],
                                 port: Int,
                                 prefix: String,
                                 interval: Duration)

case class CassandraConfiguration(hosts: List[String],
                                  port: Int,
                                  dcName: Option[String],
                                  clusterName: Option[String],
                                  defaultConsistency: Option[String],
                                  serialConsistency: Option[String],
                                  authMethod: Option[String],
                                  auth: CassandraAuthConfiguration,
                                  graphName: Option[String],
                                  defaultKeyspace: Option[String],
                                  poolingOptions: CassandraPoolingConfiguration,
                                  graphiteConf: GraphiteConfiguration)

case class CassandraAuthConfiguration(username: Option[String],
                                      password: Option[String])

case class CassandraPoolingConfiguration(local: CassandraPoolingConnections,
                                         remote: CassandraPoolingConnections,
                                         maxQueueSize: Int)

case class CassandraPoolingConnections(coreConnections: Int,
                                       maxConnections: Int,
                                       maxRequestsPerConnection: Int)


case class DseStressConfig(general: GeneralConfiguration,
                           cassandra: CassandraConfiguration,
                           config: Config
                    ) {
  def resolve[T](value: T): T = value
}
