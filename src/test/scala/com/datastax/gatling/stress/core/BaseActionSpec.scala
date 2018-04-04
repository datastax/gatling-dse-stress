package com.datastax.gatling.stress.core

import com.datastax.driver.core.ResultSet
import com.datastax.gatling.stress.base.BaseSpec
import com.datastax.gatling.stress.libs.{Cassandra, SimConfig}
import com.typesafe.config.ConfigFactory
import org.scalatest.easymock.EasyMockSugar

/**
  * Base Action Spec
  */
class BaseActionSpec extends BaseSpec {

  val NETWORK_STRATEGY = "NetworkTopologyStrategy"
  val SIMPLE_STRATEGY = "SimpleStrategy"
  val KEYSPACE_NAME = "TestKeyspace"

  val defaultConfig = ConfigFactory.load("./application.conf")
  val defaultCass = mock[Cassandra]
  val defaultAppConfig = new SimConfig(defaultConfig, "fetchBaseData", "read")

  val baseAction = new BaseActionStub(defaultCass, defaultAppConfig)

  describe("buildCreateKeyspaceQuery") {

    describe("NetworkTopologyStrategy") {

      it("should return the expected create string for NetworkTopologyStrategy and multiple Dcs") {
        val expected = s"""CREATE KEYSPACE IF NOT EXISTS "$KEYSPACE_NAME" WITH REPLICATION = { 'class': '$NETWORK_STRATEGY', 'dc1': 3, 'dc2': 1 };"""
        val result = baseAction.buildCreateNetworkTopologyKeyspaceQuery(KEYSPACE_NAME, Array(("dc1", 3), ("dc2", 1)))

        result shouldBe expected
      }

      it("should return the expected create string for NetworkTopologyStrategy and single Dc") {
        val expected = s"""CREATE KEYSPACE IF NOT EXISTS "$KEYSPACE_NAME" WITH REPLICATION = { 'class': '$NETWORK_STRATEGY', 'dc1': 3 };"""
        val result = baseAction.buildCreateNetworkTopologyKeyspaceQuery(KEYSPACE_NAME, Array(("dc1", 3)))

        result shouldBe expected
      }
    }

    it("should return the expected create string for SimpleStrategy") {
      val expected = s"""CREATE KEYSPACE IF NOT EXISTS "$KEYSPACE_NAME" WITH REPLICATION = { 'class': '$SIMPLE_STRATEGY', 'replication_factor' : 3 };"""
      val result = baseAction.buildCreateSimpleKeyspaceQuery(KEYSPACE_NAME, 3)

      result shouldBe expected
    }
  }


  describe("createKeyspace") {

    it("should throw and exception if createKeyspace connection doesn't exist") {
      the[RuntimeException] thrownBy baseAction.createKeyspace
    }

    it("should throw and exception if topology is invalid") {
      val networkKeyspaceTest = new SimConfig(defaultConfig, "createBadKeyspaceTest", "read")
      val baseActionNet = new BaseActionStub(defaultCass, networkKeyspaceTest)
      the[RuntimeException] thrownBy baseActionNet.createKeyspace
    }


    describe("NetworkTopologyStrategy") {

      val networkKeyspaceTest = new SimConfig(defaultConfig, "createNetworkKeyspaceTest", "read")
      val baseActionNet = new BaseActionStub(defaultCass, networkKeyspaceTest)

      it("should accept multiple DCs") {

        val expectedRes =
          s"""CREATE KEYSPACE IF NOT EXISTS "$KEYSPACE_NAME" WITH REPLICATION = { 'class': '$NETWORK_STRATEGY', 'dc1': 3, 'dc2': 1 };"""

        val expectedRes2 =
          s"""CREATE KEYSPACE IF NOT EXISTS "$KEYSPACE_NAME" WITH REPLICATION = { 'class': '$NETWORK_STRATEGY', 'dc2': 1, 'dc1': 3 };"""

        baseActionNet.createKeyspace
        baseActionNet.lastQuery should (be(expectedRes2) or be(expectedRes))
      }

      it("should accept single dc") {

        val networkKeyspaceTest2 = new SimConfig(defaultConfig, "createNetworkKeyspaceTest2", "read")
        val baseActionNet2 = new BaseActionStub(defaultCass, networkKeyspaceTest2)

        val expectedRes =
          s"""CREATE KEYSPACE IF NOT EXISTS "$KEYSPACE_NAME" WITH REPLICATION = { 'class': '$NETWORK_STRATEGY', 'dc1': 3 };"""

        baseActionNet2.createKeyspace
        baseActionNet2.lastQuery shouldBe expectedRes
      }
    }

    describe("SimpleStrategy") {

      val simpleKeyspaceTest = new SimConfig(defaultConfig, "createSimpleKeyspaceTest", "read")
      val baseActionNet = new BaseActionStub(defaultCass, simpleKeyspaceTest)

      it("should generate the correct query") {
        val expectedRes =
          s"""CREATE KEYSPACE IF NOT EXISTS "$KEYSPACE_NAME" WITH REPLICATION = { 'class': '$SIMPLE_STRATEGY', 'replication_factor' : 3 };"""

        baseActionNet.createKeyspace
        baseActionNet.lastQuery shouldBe expectedRes
      }
    }

    describe("disabledKeyspaceCreate") {

      val simpleKeyspaceTest = new SimConfig(defaultConfig, "createKeyspaceEnabledTest", "read")
      val baseActionNet = new BaseActionStub(defaultCass, simpleKeyspaceTest)

      it("should generate the correct query") {
        baseActionNet.createKeyspace shouldBe false
      }
    }
  }

  describe("runQueries") {

    it("should accept an array and pause between calls") {

      val queries = Array("SELECT * FROM test", "SELECT * FROM test2", "SELECT * FROM test3")

      val start = System.currentTimeMillis()
      baseAction.runQueries(queries)
      val stop = System.currentTimeMillis()

      baseAction.lastQuery shouldBe queries(2)
      (stop - start) should be > 100L
    }

    it("should accept a seq and pause between calls") {

      val queries = Seq("SELECT * FROM test", "SELECT * FROM test2", "SELECT * FROM test3")

      val start = System.currentTimeMillis()
      baseAction.runQueries(queries)
      val stop = System.currentTimeMillis()

      baseAction.lastQuery shouldBe queries(2)
      (stop - start) should be > 100L
    }

  }

}


class BaseActionStub(cass: Cassandra, appConf: SimConfig) extends BaseAction(cass, appConf) with EasyMockSugar {

  var lastQuery: String = ""

  override def buildCreateNetworkTopologyKeyspaceQuery(keyspace: String, dcRep: Array[(String, Int)]) =
    super.buildCreateNetworkTopologyKeyspaceQuery(keyspace: String, dcRep: Array[(String, Int)])

  override def buildCreateSimpleKeyspaceQuery(keyspace: String, repFactor: Int) =
    super.buildCreateSimpleKeyspaceQuery(keyspace, repFactor)

  override def createKeyspace = super.createKeyspace

  override def runQueries(queries: Array[String]) = super.runQueries(queries)

  override def runQueries(queries: Seq[String]) = super.runQueries(queries)

  override def executeQuery(query: String) = {
    this.lastQuery = query
    mock[ResultSet]
  }


}