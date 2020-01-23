package com.datastax.gatling.stress.libs

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, LatencyAwarePolicy, TokenAwarePolicy}
import com.datastax.driver.dse.DseCluster
import com.datastax.driver.dse.auth.DsePlainTextAuthProvider
import com.datastax.gatling.stress.base.BaseCassandraSpec
import com.typesafe.config.ConfigFactory


class CassandraSpec extends BaseCassandraSpec {

  val appConfig = new AppConfig()
  val defaultConfig = appConfig.loadConfig(Some("./application.conf"))
  val defaultCass = new Cassandra(defaultConfig)

  describe("Builder Defaults") {

    describe("hosts") {

      it("should accept a single host in string") {
        val hosts = defaultCass.getBuilder.getContactPoints

        hosts.size() shouldBe 1
        hosts.toString should include("127.0.0.1")
      }

      it("should accept a list of comma separated hosts") {

        val myConfig = ConfigFactory.parseString("cassandra.hosts=\"127.0.0.1,127.0.0.2\"")
        val newConfig = myConfig.withFallback(defaultConfig)
        val cass = new Cassandra(newConfig)

        val hosts = cass.getBuilder.getContactPoints

        hosts.size() shouldBe 2
        hosts.toString should (include("127.0.0.1") and include("127.0.0.2"))
      }

      it("should accept a list hosts") {

        val myConfig = ConfigFactory.parseString("cassandra.hosts.0=\"127.0.0.1\",cassandra.hosts.1=\"127.0.0.2\"")
        val newConfig = myConfig.withFallback(defaultConfig)
        val cass = new Cassandra(newConfig)

        val hosts = cass.getBuilder.getContactPoints
        hosts.size() shouldBe 2
        hosts.toString should (include("127.0.0.1") and include("127.0.0.2"))
      }

    }

    describe("port") {

      it("should be settable") {

        val hosts = defaultCass.getBuilder.getContactPoints
        hosts.toString should include("9142")

        val myConfig = ConfigFactory.parseString("cassandra.port=9142")
        val newConfig = myConfig.withFallback(defaultConfig)
        val cass2 = new Cassandra(newConfig)

        val hosts2 = cass2.getBuilder.getContactPoints
        hosts2.toString should include("9142")
      }

    }

    describe("dcName") {

      it("should default to DCAwareRR") {
        val lbPolicy = defaultCass.getBuilder.getConfiguration.getPolicies.getLoadBalancingPolicy
        lbPolicy shouldBe a[TokenAwarePolicy]
      }

    }

    describe("clusterName") {

      it("should be auto set") {
        defaultCass.getBuilder.getClusterName shouldBe "Test Cluster"
      }

      it("should be ignored if null") {
        val myConfig = ConfigFactory.parseString("cassandra.clusterName=null")
        val newConfig = myConfig.withFallback(defaultConfig)
        val cass2 = new Cassandra(newConfig)

        cass2.getBuilder.getClusterName shouldBe null
      }

      it("should be ignored if blank") {
        val myConfig = ConfigFactory.parseString("cassandra.clusterName=\"\"")
        val newConfig = myConfig.withFallback(defaultConfig)
        val cass2 = new Cassandra(newConfig)

        cass2.getBuilder.getClusterName shouldBe null
      }

    }


    describe("defaultConsistency") {

      it("should be settable") {

        val consistencyLevel = defaultCass.getBuilder.getConfiguration.getQueryOptions.getConsistencyLevel
        consistencyLevel.toString shouldBe "LOCAL_QUORUM"

        val myConfig = ConfigFactory.parseString("cassandra.defaultConsistency=\"LOCAL_ONE\"")
        val newConfig = myConfig.withFallback(defaultConfig)
        val cass2 = new Cassandra(newConfig)

        val consistencyLevel2 = cass2.getBuilder.getConfiguration.getQueryOptions.getConsistencyLevel
        consistencyLevel2.toString shouldBe "LOCAL_ONE"

      }

    }

    describe("serialConsistency") {

      it("should be settable") {

        val consistencyLevel = defaultCass.getBuilder.getConfiguration.getQueryOptions.getSerialConsistencyLevel
        consistencyLevel.toString shouldBe "LOCAL_SERIAL"

        val myConfig = ConfigFactory.parseString("cassandra.serialConsistency=\"SERIAL\"")
        val newConfig = myConfig.withFallback(defaultConfig)
        val cass2 = new Cassandra(newConfig)

        val consistencyLevel2 = cass2.getBuilder.getConfiguration.getQueryOptions.getSerialConsistencyLevel
        consistencyLevel2.toString shouldBe "SERIAL"

      }

    }

    describe("credentials") {

      it("should be ignored by default") {
        val authProvider = defaultCass.getBuilder.getConfiguration.getProtocolOptions.getAuthProvider
        authProvider shouldBe a[AuthProvider]
      }

      it("should be settable using auth section") {
        val myConfig = ConfigFactory.parseString("cassandra.auth.username=\"test\",cassandra.auth.password=\"test\"")
        val newConfig = myConfig.withFallback(defaultConfig)
        val cass2 = new Cassandra(newConfig)

        val authProvider = cass2.getBuilder.getConfiguration.getProtocolOptions.getAuthProvider
        authProvider shouldBe a[DsePlainTextAuthProvider]
      }

    }


    describe("ssl") {

      it("should be ignored by default") {
        defaultCass.getBuilder.getConfiguration.getProtocolOptions.getSSLOptions shouldBe null
      }

      it("should auto enable if system property is set") {

        System.setProperty("javax.net.ssl.trustStore", "/test.jks")

        val cass2 = new Cassandra(defaultConfig)

        cass2.getBuilder.getConfiguration.getProtocolOptions.getSSLOptions shouldBe a[SSLOptions]

      }

    }

    describe("poolingOptions") {

      describe("local") {

        val hostDistance = HostDistance.LOCAL
        val distanceString = "local"

        it("should be settable") {
          defaultCass.getBuilder.getConfiguration.getPoolingOptions.getCoreConnectionsPerHost(hostDistance) shouldBe 2
          defaultCass.getBuilder.getConfiguration.getPoolingOptions.getMaxConnectionsPerHost(hostDistance) shouldBe 8
          defaultCass.getBuilder.getConfiguration.getPoolingOptions.getMaxRequestsPerConnection(hostDistance) shouldBe 10000
        }

        it("should set max = core if max < core") {

          val myConfig = ConfigFactory.parseString("cassandra.poolingOptions." + distanceString + ".maxConnections=1")
          val newConfig = myConfig.withFallback(defaultConfig)
          val cass2 = new Cassandra(newConfig)

          cass2.getBuilder.getConfiguration.getPoolingOptions.getCoreConnectionsPerHost(hostDistance) shouldBe 2
          cass2.getBuilder.getConfiguration.getPoolingOptions.getMaxConnectionsPerHost(hostDistance) shouldBe 2
        }

      }

      describe("remote") {

        val hostDistance = HostDistance.REMOTE
        val distanceString = "remote"

        it("should be settable") {
          defaultCass.getBuilder.getConfiguration.getPoolingOptions.getCoreConnectionsPerHost(hostDistance) shouldBe 1
          defaultCass.getBuilder.getConfiguration.getPoolingOptions.getMaxConnectionsPerHost(hostDistance) shouldBe 1
          defaultCass.getBuilder.getConfiguration.getPoolingOptions.getMaxRequestsPerConnection(hostDistance) shouldBe 512
        }

        it("should set max = core if max < core") {

          val myConfig = ConfigFactory.parseString("cassandra.poolingOptions." + distanceString + ".maxConnections=0")
          val newConfig = myConfig.withFallback(defaultConfig)
          val cass2 = new Cassandra(newConfig)

          cass2.getBuilder.getConfiguration.getPoolingOptions.getCoreConnectionsPerHost(hostDistance) shouldBe 1
          cass2.getBuilder.getConfiguration.getPoolingOptions.getMaxConnectionsPerHost(hostDistance) shouldBe 1
        }

      }

      describe("maxQueueSize") {

        it("should be settable") {
          defaultCass.getBuilder.getConfiguration.getPoolingOptions.getMaxQueueSize shouldBe 512
        }

      }

    }

  }


  describe("customized_builder") {

    describe("loadBalacingPolicy") {

      it("should be capable of overriding") {

        defaultCass.getBuilder.getConfiguration.getPolicies.getLoadBalancingPolicy shouldBe a[TokenAwarePolicy]

        defaultCass.getBuilder.withLoadBalancingPolicy(LatencyAwarePolicy.builder(
          DCAwareRoundRobinPolicy.builder().build()).build()
        )

        defaultCass.getBuilder.getConfiguration.getPolicies.getLoadBalancingPolicy shouldBe a[LatencyAwarePolicy]
      }
    }

    describe("authentication") {

      it("should be capable of overriding") {

        var authProvider = defaultCass.getBuilder.getConfiguration.getProtocolOptions.getAuthProvider
        authProvider shouldBe a[AuthProvider]

        defaultCass.getBuilder.withCredentials("test", "user")

        authProvider = defaultCass.getBuilder.getConfiguration.getProtocolOptions.getAuthProvider
        authProvider shouldBe a[DsePlainTextAuthProvider]
      }
    }

  }


  describe("session") {

    it("should create a session using default values") {
      defaultCass.setSession(null)

      defaultCass.getSession.isClosed shouldBe false
      defaultCass.getSession.getCluster.getConfiguration.getPoolingOptions
          .getCoreConnectionsPerHost(HostDistance.LOCAL) shouldBe 2
    }


    it("should allow setting a custom session") {

      val myConfig = ConfigFactory.parseString("cassandra.poolingOptions.maxQueueSize=128")
      val newConfig = myConfig.withFallback(defaultConfig)
      val cass2 = new Cassandra(newConfig)

      val clusterBuilder = new DseCluster.Builder().addContactPoint("127.0.0.1").withPort(9142).build()
      cass2.setSession(clusterBuilder.connect())

      cass2.getSession.isClosed shouldBe false
      cass2.getSession.getCluster.getConfiguration.getPoolingOptions.getCoreConnectionsPerHost(HostDistance.LOCAL) shouldBe 1
    }
  }

}

