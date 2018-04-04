package com.datastax.gatling.stress.libs

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging

/**
  * Application Configuration
  */
class AppConfig extends LazyLogging {

  val defaultConf = ConfigFactory.load("default.conf")
  val gatlingConf = ConfigFactory.load("gatling.conf")

  def loadConfig(path: Option[String] = None): Config = {
    try {
      if (path.isDefined) {
        ConfigFactory.load(path.get).withFallback(defaultConf).withFallback(gatlingConf)
      } else {
        ConfigFactory.load.withFallback(defaultConf).withFallback(gatlingConf)
      }
    } catch {
      case e: Exception =>
        logger.error("Unable to load config, due to error. Error = {}", e.toString)
        throw new RuntimeException("Exiting.")
    }
  }

}
