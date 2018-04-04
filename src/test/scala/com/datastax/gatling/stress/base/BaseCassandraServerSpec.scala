package com.datastax.gatling.stress.base

import org.cassandraunit.utils.EmbeddedCassandraServerHelper

class BaseCassandraServerSpec extends BaseSpec {

  EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml", "./build/cassandraUnit", 30000L)

}
