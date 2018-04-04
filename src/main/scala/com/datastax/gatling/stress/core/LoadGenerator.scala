package com.datastax.gatling.stress.core

import com.datastax.gatling.stress.libs.SimConfig
import com.typesafe.scalalogging.LazyLogging
import io.gatling.core.Predef._
import io.gatling.core.structure.{PopulationBuilder, ScenarioBuilder}

import scala.concurrent.duration._

object LoadGenerator extends LazyLogging {

  private var usersRampTime: FiniteDuration = _
  private var usersConstantTime: FiniteDuration = _
  private var usersConstantCnt: Int = 0
  private var usersPercentage: Int = 0
  private var totalUsers: Int = 0
  private var totalSeconds: Long = 0

  /**
    * Build Scenario Load Based on a RampUp to Constant Scenario
    *
    * Configs Used:
    * - usersRampTime
    * - usersConstantTime
    * - usersConstantCnt
    * - usersPercentage
    *
    * @param scenario
    * @param appConf
    * @return
    */
  def rampUpToPercentage(scenario: ScenarioBuilder, appConf: SimConfig): PopulationBuilder = {

    try {

      usersRampTime = appConf.getSimulationConfDuration("usersRampTime")
      usersConstantTime = appConf.getSimulationConfDuration("usersConstantTime")

      usersConstantCnt = appConf.getSimulationConfInt("usersConstantCnt")
      usersPercentage = appConf.getSimulationConfInt("usersPercentage")

      totalUsers = (usersConstantCnt * (usersPercentage * .01)).toInt

    } catch {
      case e: Exception =>
        logger.error(s"Unable to get configuration for building load profile for {}. Check configs for: " +
            "usersRampTime, usersConstantTime, usersConstantCnt, usersPercentage. Exception: {}", scenario.name, e.toString)
        System.exit(1)
    }

    if (totalUsers == 1) {

      logger.debug("Building rampUpToPercentage load for scenario: {}. Scenario Users is 1 skipping Ramp Time, Constant Time: {}," +
          " Scenario Percentage: {}, Total Simulation Users: {}",
        scenario.name, usersConstantCnt, usersConstantTime.toString, usersPercentage, totalUsers
      )

      scenario.inject(
        constantUsersPerSec(totalUsers) during usersConstantTime
      )

    } else {

      logger.debug("Building rampUpToPercentage load for scenario: {}. Scenario Users: {}, Ramp time: {}, Constant Time: {}," +
          " Scenario Percentage: {}, Total Simulation Users: {}",
        scenario.name, usersConstantCnt, usersRampTime.toString, usersConstantTime.toString, usersPercentage, totalUsers
      )

      scenario.inject(
        rampUsersPerSec(1) to totalUsers during usersRampTime,
        constantUsersPerSec(totalUsers) during usersConstantTime
      )
    }

  }


  /**
    * Ramp Up from 0 users to n users over x time and retain constant n users for x time
    *
    * Config Used
    * - usersRampTime
    * - usersConstantCnt
    * - usersConstantTime
    *
    * @param scenario
    * @param appConf
    * @return
    */
  def rampUpToConstant(scenario: ScenarioBuilder, appConf: SimConfig): PopulationBuilder = {

    try {

      usersRampTime = appConf.getSimulationConfDuration("usersRampTime")
      usersConstantCnt = appConf.getSimulationConfInt("usersConstantCnt")
      usersConstantTime = appConf.getSimulationConfDuration("usersConstantTime")

    } catch {
      case e: Exception =>
        logger.error(
          "Unable to get configuration for building load profile for {}. Check configs for: usersRampTime, " +
              "usersConstantCnt, usersConstantTime.  Exception: {}",
          scenario.name, e.toString)
        System.exit(1)
    }

    if (totalUsers == 1) {

      logger.debug(
        "Building rampUpToConstant load for scenario: {}. User cnt is 1 skipping Ramp Time, Constant Time: {}",
        scenario.name, usersRampTime.toString, usersConstantTime.toString(), usersConstantCnt
      )

      scenario.inject(
        constantUsersPerSec(usersConstantCnt) during usersConstantTime
      )

    } else {

      logger.debug(
        "Building rampUpToConstant load for scenario: {}. Ramp time: {}, Constant Time: {}, Users: {}",
        scenario.name, usersRampTime.toString, usersConstantTime.toString(), usersConstantCnt
      )

      scenario.inject(
        rampUsersPerSec(1) to usersConstantCnt during usersRampTime,
        constantUsersPerSec(usersConstantCnt) during usersConstantTime
      )
    }
  }


  /**
    * Auto determines the duration of the scenario based on the usersTotal wanted and the usersConstantCnt
    *
    * Config Params Used:
    * - usersConstantCnt
    * - usersTotal
    *
    * @param scenario
    * @param appConf
    * @return
    */
  def constantToTotal(scenario: ScenarioBuilder, appConf: SimConfig): PopulationBuilder = {

    try {
      usersConstantCnt = appConf.getSimulationConfInt("usersConstantCnt")
      totalUsers = appConf.getSimulationConfInt("usersTotal")
    } catch {
      case e: Exception =>
        logger.error(
          "Unable to get configuration for building load profile for {}. Check configs for: usersConstantCnt, " +
              "usersTotal. Exception: {}",
          scenario.name, e.toString
        )
        System.exit(1)
    }

    totalSeconds = totalUsers / usersConstantCnt

    logger.debug(
      "Building constantToTotal load for scenario: {}. Total Users: {}, Constant Count: {} over {} seconds",
      scenario.name, totalUsers.toString, usersConstantCnt.toString, totalSeconds.toString
    )

    scenario.inject(
      constantUsersPerSec(usersConstantCnt) during totalSeconds.seconds
    )

  }


  /**
    * Run only 1 user once for debugging
    *
    * @param scenario
    */
  def runOnlyOnce(scenario: ScenarioBuilder): PopulationBuilder = {

    logger.debug("Building runOnlyOnce load for scenario {} of Total User: 1 over 1 second", scenario.name)

    scenario.inject(
      constantUsersPerSec(1) during 1.seconds
    )
  }
}
