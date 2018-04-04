package com.datastax.gatling.stress.libs

import java.util.concurrent.TimeUnit

import com.datastax.gatling.stress.base.BaseSpec
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration.FiniteDuration


/**
  * Sim Config Spec
  */
class SimConfigSpec extends BaseSpec {

  val simName = "testSimConfig"

  val conf: Config = ConfigFactory.load

  val scenarioConfigExists = new SimConfig(conf, simName, "includesTable")
  val scenarioConfigDefaults = new SimConfig(conf, simName, "missingTable")


  describe("getConf") {
    val scenarioConfig = new SimConfig(conf, simName, "includesTable")

    it("should be the entire config") {
      scenarioConfig.getConf shouldBe a[Config]
      scenarioConfig.getConf.hasPath("cassandra") shouldBe true
    }
  }


  describe("getSimulationConfStr") {

    it("should load from scenario section if present") {
      scenarioConfigExists.getSimulationConfStr("table") shouldBe "table1"
    }

    it("should load from defaults section if scenario section is not present") {
      scenarioConfigDefaults.getSimulationConfStr("table") shouldBe "default"
    }

    it("should throw and exception if defaults section and scenario section is not present") {
      the[RuntimeException] thrownBy scenarioConfigDefaults.getSimulationConfStr("missing")
    }
  }


  describe("getSimulationConfBool") {

    it("should load from scenario section if present") {
      scenarioConfigExists.getSimulationConfBool("bool") shouldBe true
    }

    it("should load from defaults section if scenario section is not present") {
      scenarioConfigDefaults.getSimulationConfBool("bool") shouldBe true
    }

    it("should throw and exception if defaults section and scenario section is not present") {
      the[RuntimeException] thrownBy scenarioConfigDefaults.getSimulationConfBool("missing")
    }
  }


  describe("getSimulationConfInt") {

    it("should load from scenario section if present") {
      scenarioConfigExists.getSimulationConfInt("int") shouldBe 1
    }

    it("should load from defaults section if scenario section is not present") {
      scenarioConfigDefaults.getSimulationConfInt("int") shouldBe 1
    }

    it("should throw and exception if defaults section and scenario section is not present") {
      the[RuntimeException] thrownBy scenarioConfigDefaults.getSimulationConfInt("missing")
    }
  }


  describe("getSimulationConfList") {

    val list: List[String] = List("1", "2", "3")

    it("should load from scenario section if present") {
      scenarioConfigExists.getSimulationConfStrList("list") shouldBe list
    }

    it("should load from defaults section if scenario section is not present") {
      scenarioConfigDefaults.getSimulationConfStrList("list") shouldBe list
    }

    it("should throw and exception if defaults section and scenario section is not present") {
      the[RuntimeException] thrownBy scenarioConfigDefaults.getSimulationConfStrList("missing")
    }
  }


  describe("getSimulationConfDuration") {

    it("should load from scenario section if present") {
      val dur = scenarioConfigExists.getSimulationConfDuration("duration")
      dur shouldBe a[FiniteDuration]
      dur shouldBe new FiniteDuration(1, TimeUnit.MINUTES)
    }

    it("should load from defaults section if scenario section is not present") {
      val dur = scenarioConfigDefaults.getSimulationConfDuration("duration")
      dur shouldBe a[FiniteDuration]
      dur shouldBe new FiniteDuration(1, TimeUnit.MINUTES)
    }

    it("should throw and exception if defaults section and scenario section is not present") {
      the[RuntimeException] thrownBy scenarioConfigDefaults.getSimulationConfDuration("missing")
    }
  }


  describe("cassandraConfExists") {

    it("should return true if exists") {
      scenarioConfigDefaults.cassandraConfExists("hosts") shouldBe true
    }

    it("should return false if does not exist") {
      scenarioConfigDefaults.cassandraConfExists("nope") shouldBe false
    }

  }


  describe("generalConfExists") {

    it("should return true if exists") {
      scenarioConfigDefaults.generalConfExists("dataDir") shouldBe true
    }

    it("should return false if does not exist") {
      scenarioConfigDefaults.generalConfExists("nope") shouldBe false
    }

  }


  describe("defaultConfExists") {

    it("should return true if exists") {
      scenarioConfigDefaults.defaultConfExists("table") shouldBe true
    }

    it("should return false if does not exist") {
      scenarioConfigDefaults.defaultConfExists("nope") shouldBe false
    }

  }


  describe("scenarioConfExists") {

    it("should return true if exists") {
      scenarioConfigExists.scenarioConfExists("table") shouldBe true
    }

    it("should return false if does not exist") {
      scenarioConfigExists.scenarioConfExists("nope") shouldBe false
    }

  }


  describe("getCassandraConfBool") {

    it("should return true if exists") {
      scenarioConfigExists.getCassandraConfBool("exists") shouldBe true
    }

    it("should return false if does not exist") {
      scenarioConfigExists.getCassandraConfBool("nope") shouldBe false
    }

  }


  describe("getCassandraConfStr") {

    it("should return proper string if exists") {
      scenarioConfigExists.getCassandraConfStr("hosts") shouldBe "127.0.0.1"
    }

    it("should return empty string if does not exist") {
      scenarioConfigExists.getCassandraConfStr("nope") shouldBe ""
    }

  }


  describe("getCasandraConfInt") {

    it("should return int if exists") {
      scenarioConfigExists.getCasandraConfInt("port") shouldBe 9142
    }

    it("should return 0 if does not exist") {
      scenarioConfigExists.getCasandraConfInt("nope") shouldBe 0
    }

  }


  describe("getCassandraConfList") {

    it("should return list if exists") {
      scenarioConfigExists.getCassandraConfList("list") shouldBe a[java.util.ArrayList[_]]
    }

    it("should return null if does not exist") {
      scenarioConfigExists.getCassandraConfList("nope") shouldBe None
    }

  }


  describe("getGeneralConfBool") {

    it("should return true if exists") {
      scenarioConfigExists.getGeneralConfBool("boolean") shouldBe true
    }

    it("should return false if does not exist") {
      scenarioConfigExists.getGeneralConfBool("nope") shouldBe false
    }

  }


  describe("getGeneralConfStr") {

    it("should return string if exists") {
      scenarioConfigExists.getGeneralConfStr("string") shouldBe "str"
    }

    it("should return empty string if does not exist") {
      scenarioConfigExists.getGeneralConfStr("nope") shouldBe ""
    }

  }


  describe("getGeneralConfInt") {

    it("should return 1 if exists") {
      scenarioConfigExists.getGeneralConfInt("int") shouldBe 1
    }

    it("should return 0 if does not exist") {
      scenarioConfigExists.getGeneralConfInt("nope") shouldBe 0
    }

  }


  describe("getGeneralConfList") {

    it("should return list if exists") {
      scenarioConfigExists.getGeneralConfList("list") shouldBe a[java.util.ArrayList[_]]
    }

    it("should return null if does not exist") {
      scenarioConfigExists.getGeneralConfList("nope") shouldBe None
    }

  }


}

