package com.datastax.gatling.stress.utils

import com.datastax.gatling.stress.base.BaseSpec


class SimulationFinderSpec extends BaseSpec {

  val cFinder = SimulationFinder

  val simName = "SimpleStatementSimulation"
  val exampleSimName = "ExampleSimulation"

  describe("getSims") {

    val sims = cFinder.getSimList

    it(s"should include $simName") {
      sims should include(simName)
    }

    it(s"should not include $exampleSimName") {
      sims should not include exampleSimName
    }

  }

  describe("findSimulation") {

    it(s"should return count of 1 when found") {
      val sims = cFinder.findSimulation(simName)
      sims.length shouldBe 1
    }

    it(s"should return count of 0 when not found") {
      val sims = cFinder.findSimulation(exampleSimName)
      sims.length shouldBe 0
    }

  }

}
