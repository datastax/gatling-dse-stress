package com.datastax.gatling.stress.utils

import com.google.common.reflect.ClassPath

import scala.collection.mutable.ListBuffer

object SimulationFinder {

  private final val SIM_PACKAGE = "sims"

  private final val SIMULATION_POSTFIX = "simulation"

  private def getAllSims = {

    val cp = ClassPath.from(Thread.currentThread.getContextClassLoader)
    val classes = cp.getTopLevelClassesRecursive(SIM_PACKAGE)
    val simList = new ListBuffer[String]

    if (!classes.isEmpty) {
      classes.toArray.foreach { x =>
        if (x.toString.toLowerCase.endsWith(SIMULATION_POSTFIX)) {
          simList.append(x.toString)
        }
      }
    }

    simList.toArray.sorted
  }

  /**
    * Get All Simulations in ClassPath
    *
    * @return
    */
  def getSimList: String = {

    val allSims = getAllSims

    if (allSims.isEmpty) {
      return "No simulations found."
    }

    val output = new StringBuilder
    output.append("Available sims:\n")

    for (a <- allSims) {
      output.append("\t - " + a + "\n")
    }

    output.toString()
  }


  /**
    * Find Simulation by name
    *
    * @param simName
    * @return
    */
  def findSimulation(simName: String): Array[String] = {

    val foundMatchingSims = new ListBuffer[String]

    for (s <- getAllSims) {
      if (s.toLowerCase.contains(simName.toLowerCase)) {
        foundMatchingSims.append(s)
      }
    }

    foundMatchingSims.toArray
  }

}
