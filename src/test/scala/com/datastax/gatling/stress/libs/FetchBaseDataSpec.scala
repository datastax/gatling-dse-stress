package com.datastax.gatling.stress.libs

import java.io.FileWriter

import com.datastax.driver.core.{Host, TokenRange}
import com.datastax.gatling.stress.base.BaseCassandraServerSpec
import com.typesafe.config.ConfigFactory
import org.scalatest.easymock.EasyMockSugar

import scala.collection.JavaConverters._
import scala.util.Random

/**
  * SPec fo Fetch Base Data Class
  */
class FetchBaseDataSpec extends BaseCassandraServerSpec {

  val defaultConfig = ConfigFactory.load("./application.conf")
  val defaultCass = new Cassandra(defaultConfig)
  val defaultAppConfig = new SimConfig(defaultConfig, "fetchBaseData", "read")

  createTestData()

  describe("getRanges") {

    val fetchBaseData = new FetchBaseDataStub(defaultAppConfig, defaultCass)

    it("should return a set with more thank one TokenRang") {
      val ranges = fetchBaseData.getRanges
      ranges shouldBe a[Set[_]]
      ranges.size should be > 1
    }

    it("should return same set if called twice") {
      val ranges = fetchBaseData.getRanges
      ranges shouldBe a[Set[_]]
      ranges.size should be > 1

      val ranges2 = fetchBaseData.getRanges
      ranges shouldBe ranges2
    }
  }

  describe("setHostRanges") {

    it("should start with 0 tokens and after run be greater than 0 ") {

      val fetchBaseData = new FetchBaseDataStub(defaultAppConfig, defaultCass)
      val hosts: scala.collection.mutable.Set[Host] = fetchBaseData.getHosts

      fetchBaseData.getFoundTokenRanges.size should be < 1
      fetchBaseData.setHostRanges(hosts.toList.head)
      fetchBaseData.getFoundTokenRanges.size should be > 0
    }

    it("should skip dcNames that don't match config") {
      val myConfig = ConfigFactory.parseString("cassandra.dcName=missing")
      val newConfig = myConfig.withFallback(defaultConfig)

      val defaultAppConfig = new SimConfig(newConfig, "fetchBaseData", "read")
      val fetchBaseData = new FetchBaseDataStub(defaultAppConfig, defaultCass)
      val hosts: scala.collection.mutable.Set[Host] = fetchBaseData.getHosts

      fetchBaseData.setHostRanges(hosts.toList.head)
      fetchBaseData.getFoundTokenRanges.size should be < 1

    }

    it("should default to vnode size if configs are set to high") {

      val defaultAppConfig = new SimConfig(defaultConfig, "fetchBaseData", "badTokenRangesPerHost")
      val fetchBaseData = new FetchBaseDataStub(defaultAppConfig, defaultCass)
      val hosts: scala.collection.mutable.Set[Host] = fetchBaseData.getHosts

      fetchBaseData.getTokenRangesPerHost shouldBe defaultAppConfig.getSimulationConfInt("tokenRangesPerHost")
      fetchBaseData.setHostRanges(hosts.toList.head)

      val hostSet = defaultCass.getSession.getCluster.getMetadata.getAllHosts.asScala
      val vnodesCnt = hostSet.toList.head.getTokens.size

      fetchBaseData.getTokenRangesPerHost shouldBe vnodesCnt
    }


    it("multiple calls should skip already found tokens ranges") {

      val fetchBaseData = new FetchBaseDataStub(defaultAppConfig, defaultCass)
      val hosts: scala.collection.mutable.Set[Host] = fetchBaseData.getHosts

      fetchBaseData.setHostRanges(hosts.toList.head)
      val getFoundTokenRanges_v1 = fetchBaseData.getFoundTokenRanges
      fetchBaseData.setHostRanges(hosts.toList.head)
      val getFoundTokenRanges_v2 = fetchBaseData.getFoundTokenRanges

      getFoundTokenRanges_v1.size shouldBe getFoundTokenRanges_v2.size
    }

  }


  describe("getRows") {

    it("should return rows") {
      val fetchBaseData = new FetchBaseDataStub(defaultAppConfig, defaultCass)

      fetchBaseData.setHostRanges(fetchBaseData.getHosts.toList.head)
      val getFoundTokenRanges = fetchBaseData.getFoundTokenRanges
      val unwrappedTokenRange = getFoundTokenRanges.head.unwrap()

      val rows = fetchBaseData.getRowColumns(unwrappedTokenRange.asScala.head)

      rows.size should be > 0
    }
  }


  describe("writeRowsToFile") {

    it("should succeed when rows are passed") {
      val fetchBaseData = new FetchBaseDataStub(defaultAppConfig, defaultCass)

      val hosts: scala.collection.mutable.Set[Host] = fetchBaseData.getHosts

      fetchBaseData.setHostRanges(hosts.toList.head)
      val getFoundTokenRanges = fetchBaseData.getFoundTokenRanges
      val unwrappedTokenRange = getFoundTokenRanges.head.unwrap()

      val rows = fetchBaseData.getRowColumns(unwrappedTokenRange.asScala.head)

      rows.size should be > 0

      val res = fetchBaseData.writeRowsToFile(rows)
      res shouldBe true
    }

    it("should fail when no rows are passed") {
      val fetchBaseData = new FetchBaseDataStub(defaultAppConfig, defaultCass)
      val noRows = Seq[String]()

      val res = fetchBaseData.writeRowsToFile(noRows)
      res shouldBe false
    }
  }


  def createTestData(): Unit = {

    val keyspace = defaultAppConfig.getSimulationConfStr("keyspace")
    val table = defaultAppConfig.getSimulationConfStr("table")

    val keyspaceQuery =
      s"""
         |CREATE KEYSPACE IF NOT EXISTS $keyspace WITH
         |REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
      """.stripMargin

    val tableQuery =
      s"""
         |CREATE TABLE IF NOT EXISTS $keyspace.$table (
         |      id int,
         |      str text,
         |      PRIMARY KEY (id));
      """.stripMargin

    defaultCass.getSession.execute(keyspaceQuery)
    defaultCass.getSession.execute(tableQuery)

    Thread sleep 1000

    for (i <- 1 to 1000) {
      val str = Random.alphanumeric.take(5).mkString
      val q = s"""INSERT INTO $keyspace.$table (id, str) VALUES ($i, '$str')"""

      defaultCass.getSession.execute(q)
    }

  }

}


class FetchBaseDataStub(conf: SimConfig, cass: Cassandra) extends FetchBaseData(conf, cass) with EasyMockSugar {

  //  override lazy val logger = Logger(logr)

  override def getRanges = super.getRanges

  override val fw = mock[FileWriter]

  override def setHostRanges(host: Host): Unit = super.setHostRanges(host: Host)

  override def getRowColumns(tk: TokenRange) = super.getRowColumns(tk: TokenRange)

  override def writeRowsToFile(rows: Seq[String]) = super.writeRowsToFile(rows: Seq[String])

  def getFoundTokenRanges = this.foundTokenRanges

  def getHosts = this.allHosts

  def getTokenRangesPerHost = this.tokenRangesPerHost
}