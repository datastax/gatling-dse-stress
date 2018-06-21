package com.datastax.gatling.stress.config


object DseStressConfKeys {

  object general {
    private val base = "general."

    val dataDir = base + "dataDir"
  }

  object cassandra {
    private val base = "cassandra."

    val hosts = base + "hosts"
    val port = base + "port"
    val dcName = base + "dcName"
    val clusterName = base + "clusterName"
    val defaultKeyspace = base + "defaultKeyspace"
    val defaultConsistency = base + "defaultConsistency"
    val serialConsistency = base + "serialConsistency"
    val authMethod = base + "authMethod"

    object auth {
      private val auth = base + "auth."
      val username = auth + "username"
      val password = auth + "password"
    }

    object poolingOptions {
      private val poolingOptions = base + "poolingOptions."

      object local {
        private val location = "local."
        val coreConnections = poolingOptions + location + "coreConnections"
        val maxConnections = poolingOptions + location + "maxConnections"
        val maxRequests = poolingOptions + location + "maxRequestsPerConnection"
      }

      object remote {
        private val location = "remote."
        val coreConnections = poolingOptions + location + "coreConnections"
        val maxConnections = poolingOptions + location + "maxConnections"
        val maxRequestsPerConnection = poolingOptions + location + "maxRequestsPerConnection"
      }

      val maxQueueSize = poolingOptions + "maxQueueSize"
    }

    val graphName = base + "graphName"

  }

}
