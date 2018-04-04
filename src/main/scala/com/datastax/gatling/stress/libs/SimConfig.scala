package com.datastax.gatling.stress.libs

import java.util.concurrent.TimeUnit._

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration


/**
  * SimConfig helper class
  *
  * @param conf
  * @param simName
  * @param scenarioName
  */
class SimConfig(var conf: Config, var simName: String, var scenarioName: String) extends LazyLogging {

  private val CONF_SIM_SECTION = "simulations"
  private val CONF_SIM_DEFAULT = "defaults"
  private val CONF_CASSANDRA = "cassandra"
  private val CONF_GENERAL = "general"

  private val PERIOD_CHAR = "."

  private val scenarioConf: Config = conf.getConfig(CONF_SIM_SECTION + PERIOD_CHAR + simName + PERIOD_CHAR + scenarioName)
  private val defaultConf: Config = conf.getConfig(CONF_SIM_SECTION + PERIOD_CHAR + simName + PERIOD_CHAR + CONF_SIM_DEFAULT)

  /**
    * Get the base Config library to access full configurations
    *
    * @return
    */
  def getConf: Config = this.conf

  def getSimName = simName

  def getScenarioName = scenarioName


  /**
    * Check if the scenario path exists and is not null
    *
    * @param id
    * @return
    */
  def scenarioConfExists(id: String): Boolean = {
    conf.hasPath(getIdPath(id))
  }

  /**
    * Check if the simulation default path exists and is not null
    *
    * @param id
    * @return
    */
  def defaultConfExists(id: String): Boolean = {
    conf.hasPath(getIdPath(id, default = true))
  }


  /**
    * Check if the cassandra path exists and is not null
    *
    * @param id
    * @return
    */
  def cassandraConfExists(id: String): Boolean = {
    conf.hasPath(CONF_CASSANDRA + PERIOD_CHAR + id)
  }


  /**
    * Check if the general path exists and is not null
    *
    * @param id
    * @return
    */
  def generalConfExists(id: String): Boolean = {
    conf.hasPath(CONF_GENERAL + PERIOD_CHAR + id)
  }


  /**
    * Get Sim Conf String
    *
    * @param id
    * @return
    */
  def getSimulationConfStr(id: String): String = {

    var confVal = ""

    try {
      if (scenarioConfExists(id)) {
        confVal = scenarioConf.getString(id)
        logPathFound(getIdPath(id), confVal)
      } else if (defaultConfExists(id)) {
        confVal = defaultConf.getString(id)
        logPathFound(getIdPath(id), confVal, default = true)
      } else {
        logSimPathError(id)
      }
    } catch {
      case e: Exception =>
        logInvalidSimConfigType(id, e)
    }

    confVal
  }

  /**
    * Get Sim Conf Int
    *
    * @param id
    * @return
    */
  def getSimulationConfInt(id: String): Int = {

    var confVal = 0

    try {
      if (scenarioConfExists(id)) {
        confVal = scenarioConf.getInt(id)
        logPathFound(getIdPath(id), confVal)
      } else if (defaultConfExists(id)) {
        confVal = defaultConf.getInt(id)
        logPathFound(getIdPath(id), confVal, default = true)
      } else {
        logSimPathError(id)
      }
    } catch {
      case e: Exception =>
        logInvalidSimConfigType(id, e)
    }

    confVal
  }

  /**
    * Get Sim Conf Bool
    *
    * @param id
    * @return
    */
  def getSimulationConfBool(id: String): Boolean = {

    var confVal = false

    try {
      if (scenarioConfExists(id)) {
        confVal = scenarioConf.getBoolean(id)
        logPathFound(getIdPath(id), confVal)
      } else if (defaultConfExists(id)) {
        confVal = defaultConf.getBoolean(id)
        logPathFound(getIdPath(id), confVal, default = true)
      } else {
        logSimPathError(id)
      }
    } catch {
      case e: Exception =>
        logInvalidSimConfigType(id, e)
    }

    confVal
  }


  /**
    * Get Sim Conf List
    *
    * @param id
    * @return
    */
  def getSimulationConfStrList(id: String): List[String] = {

    var confVal: List[String] = null

    try {
      if (scenarioConfExists(id)) {
        confVal = scenarioConf.getStringList(id).asScala.toList
        logPathFound(getIdPath(id), confVal)
      } else if (defaultConfExists(id)) {
        confVal = defaultConf.getStringList(id).asScala.toList
        logPathFound(getIdPath(id), confVal, default = true)
      } else {
        logSimPathError(id)
      }
    } catch {
      case e: Exception =>
        logInvalidSimConfigType(id, e)
    }

    confVal
  }

  /**
    * Get Sim Conf Int
    *
    * @param id
    * @return
    */
  def getSimulationConfDuration(id: String): FiniteDuration = {

    var confVal: FiniteDuration = new FiniteDuration(0, SECONDS)

    try {
      if (scenarioConfExists(id)) {
        confVal = scenarioConf.getDuration(id)
        logPathFound(getIdPath(id), confVal)
      } else if (defaultConfExists(id)) {
        confVal = defaultConf.getDuration(id)
        logPathFound(getIdPath(id), confVal, default = true)
      } else {
        logSimPathError(id)
      }
    } catch {
      case e: Exception =>
        logInvalidSimConfigType(id, e)
    }

    confVal
  }

  /**
    * Get Sim Conf Int
    *
    * @param id
    * @return
    */
  def getSimulationConfDouble(id: String): Double = {

    var confVal: Double = 0.0

    try {
      if (scenarioConfExists(id)) {
        confVal = scenarioConf.getDouble(id)
        logPathFound(getIdPath(id), confVal)
      } else if (defaultConfExists(id)) {
        confVal = defaultConf.getDouble(id)
        logPathFound(getIdPath(id), confVal, default = true)
      } else {
        logSimPathError(id)
      }
    } catch {
      case e: Exception =>
        logInvalidSimConfigType(id, e)
    }

    confVal
  }


  /**
    * Get Cass Conf Bool
    *
    * @param id
    * @return
    */
  def getCassandraConfBool(id: String): Boolean = {
    if (cassandraConfExists(id)) {
      conf.getBoolean(CONF_CASSANDRA + PERIOD_CHAR + id)
    } else {
      false
    }
  }


  /**
    * Get Cass Conf String
    *
    * @param id
    * @return
    */
  def getCassandraConfStr(id: String): String = {
    if (cassandraConfExists(id)) {
      conf.getString(CONF_CASSANDRA + PERIOD_CHAR + id)
    } else {
      ""
    }
  }

  /**
    * Get Cass Conf Int
    *
    * @param id
    * @return
    */
  def getCasandraConfInt(id: String): Int = {
    if (cassandraConfExists(id)) {
      conf.getInt(CONF_CASSANDRA + PERIOD_CHAR + id)
    } else {
      0
    }
  }

  /**
    * Get Cass Conf List
    * Note: must convert to type wanted from AnyRef
    *
    * @param id
    * @return
    */
  def getCassandraConfList(id: String): AnyRef = {
    if (cassandraConfExists(id)) {
      conf.getAnyRefList(CONF_CASSANDRA + PERIOD_CHAR + id)
    } else {
      None
    }
  }


  /**
    * Get General Conf Bool
    *
    * @param id
    * @return
    */
  def getGeneralConfBool(id: String): Boolean = {
    if (generalConfExists(id)) {
      conf.getBoolean(CONF_GENERAL + PERIOD_CHAR + id)
    } else {
      false
    }
  }


  /**
    * Get General Conf String
    *
    * @param id
    * @return
    */
  def getGeneralConfStr(id: String): String = {
    if (generalConfExists(id)) {
      conf.getString(CONF_GENERAL + PERIOD_CHAR + id)
    } else {
      ""
    }
  }


  /**
    * Get General Conf Int
    *
    * @param id
    * @return
    */
  def getGeneralConfInt(id: String): Int = {
    if (generalConfExists(id)) {
      conf.getInt(CONF_GENERAL + PERIOD_CHAR + id)
    } else {
      0
    }
  }

  /**
    * Get General Conf List
    * Note: must convert to type wanted from AnyRef
    *
    * @param id
    * @return
    */
  def getGeneralConfList(id: String): AnyRef = {
    if (generalConfExists(id)) {
      conf.getAnyRefList(CONF_GENERAL + PERIOD_CHAR + id)
    } else {
      None
    }
  }

  def getSimulationConf: Config = {
    conf.getConfig(Array(CONF_SIM_SECTION, this.simName).mkString(PERIOD_CHAR))
  }


  def getCassandraConf: Config = {
    conf.getConfig(CONF_CASSANDRA)
  }


  /**
    * Get Path of Id
    *
    * @param id
    * @param default
    * @return
    */
  private def getIdPath(id: String, default: Boolean = false): String = {
    var path = Array[String]()
    if (!default) {
      path = Array(CONF_SIM_SECTION, this.simName, this.scenarioName, id)
    } else {
      path = Array(CONF_SIM_SECTION, this.simName, CONF_SIM_DEFAULT, id)
    }
    path.mkString(PERIOD_CHAR)
  }

  /**
    * Conf Read Error
    *
    * @param id
    */
  private def logSimPathError(id: String) = {
    logger.error(s"Unable to load simulation scenario path: ${getIdPath(id)} or default path: " +
        s"${getIdPath(id, default = true)}. Please check application.conf. Exiting...")
    throw new RuntimeException("Invalid configuration file")
  }


  private def logInvalidSimConfigType(id: String, ex: Exception) = {
    logger.error(s"Invalid invalid Config Type at ${getIdPath(id)} or ${getIdPath(id, default = true)}. " +
        s"Please check application.conf. Reason ${ex.getMessage}. Exiting.")
    throw new RuntimeException("Invalid configuration file")
  }


  private def logPathFound(path: String, confVal: Any, default: Boolean = false) = {
    if (!default) {
      logger.trace("Found simulation scenario path: {}, value: {}", path, confVal.toString)
    } else {
      logger.trace("Unable to load scenario path {}, using default {}", path, confVal.toString)
    }
  }


  /**
    * Convert human times to FiniteDuration
    *
    * @param d
    * @return
    */
  private implicit def asFiniteDuration(d: java.time.Duration): FiniteDuration = {
    scala.concurrent.duration.Duration.fromNanos(d.toNanos)
  }

}
