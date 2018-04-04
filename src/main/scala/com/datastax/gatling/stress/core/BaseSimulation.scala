package com.datastax.gatling.stress.core

import com.datastax.gatling.plugin.DseProtocolBuilder
import com.datastax.gatling.plugin.DsePredef._
import com.datastax.gatling.stress.libs.{AppConfig, Cassandra, SimConfig}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import io.gatling.core.scenario.Simulation


abstract class BaseSimulation extends Simulation with LazyLogging {

  private final val CONF_DATA_FILE = "dataFile"
  private final val CONF_DATA_DIR = "dataDir"

  protected val appConfig: AppConfig = new AppConfig
  protected var conf: Config = appConfig.loadConfig()

  lazy val cass = new Cassandra(conf)

  /**
    * @deprecated use cqlProtocol instead
    */
  lazy val cqlConfig: DseProtocolBuilder = cqlProtocol

  lazy val graphProtocol: DseProtocolBuilder = dseProtocolBuilder.session(cass.getSession)
  lazy val cqlProtocol: DseProtocolBuilder = dseProtocolBuilder.session(cass.getSession)

  lazy val loadGenerator: LoadGenerator.type = LoadGenerator


  /**
    * When the simulation finishes, close the cassandra session
    */
  after {
    if (!cass.getSession.isClosed) {
      cass.getSession.close()
    }
  }

  /**
    * Get Data Path for Feed from Config
    *
    * @param appConfig
    * @return
    */
  protected def getDataPath(appConfig: SimConfig): String = {
    appConfig.getGeneralConfStr(CONF_DATA_DIR) + "/" + appConfig.getSimulationConfStr(CONF_DATA_FILE)
  }

}