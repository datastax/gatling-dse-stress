package com.datastax.gatling.stress.utils

/**
  * Holder for Starter Configurations for Galting
  *
  * @param mode
  * @param noReports
  * @param reportsOnly
  * @param resultsFolder
  * @param runDescription
  * @param simulation
  */
case class GatlingRunnerConfig(mode: String = "",
                               noReports: Boolean = false,
                               reportsOnly: String = "",
                               resultsFolder: String = "",
                               runDescription: String = "",
                               simulation: String = "",
                               confShow: String = "all"
                              )