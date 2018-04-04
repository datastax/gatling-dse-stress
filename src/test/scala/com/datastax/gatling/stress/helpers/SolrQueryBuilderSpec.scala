package com.datastax.gatling.stress.helpers

import com.datastax.gatling.stress.base.BaseSpec

class SolrQueryBuilderSpec extends BaseSpec {


  describe("JSON") {

    val default_query_res_json = """{"q":"*:*""""

    describe("withSort") {
      it("should accept a field and order") {
        val solrQuery = new SolrQueryBuilder().withSort("field", "asc").build
        solrQuery shouldBe s"""$default_query_res_json,"sort":"field asc"}"""
      }

      it("should accept multiple fields and orders") {
        val solrQuery = new SolrQueryBuilder().withSort("field", "asc").withSort("field2", "desc").build
        solrQuery shouldBe s"""$default_query_res_json,"sort":"field asc,field2 desc"}"""
      }

      it("should accept a map of fields and orders") {
        val solrQuery = new SolrQueryBuilder().withSort(Map("field" -> "asc", "field2" -> "desc")).build
        solrQuery shouldBe s"""$default_query_res_json,"sort":"field asc,field2 desc"}"""
      }

    }


    describe("withLimit") {
      it("should accept a limit") {
        val solrQuery = new SolrQueryBuilder().withLimit(100).build
        solrQuery shouldBe s"""$default_query_res_json,"limit":"100"}"""
      }
    }

    describe("withQuery") {
      it("should accept a custom query") {
        val solrQuery = new SolrQueryBuilder().withQuery("test:me").build
        solrQuery shouldBe s"""{"q":"test:me"}"""
      }
    }

    describe("withQueryName") {
      it("should accept a custom query") {
        val solrQuery = new SolrQueryBuilder().withQueryName("test").build
        solrQuery shouldBe s"""$default_query_res_json,"query.name":"test"}"""
      }
    }


    describe("withFilterQuery") {
      it("should accept a filter query with no options") {
        val solrQuery = new SolrQueryBuilder().withFilterQuery("test:me").build
        solrQuery shouldBe s"""$default_query_res_json,"fq":["test:me"]}"""
      }

      it("should accept multiple filter queries with no options") {
        val solrQuery = new SolrQueryBuilder().withFilterQuery("test:me").withFilterQuery("test:me").build
        solrQuery shouldBe s"""$default_query_res_json,"fq":["test:me","test:me"]}"""
      }

      it("should accept a filter query with caching off options") {
        val solrQuery = new SolrQueryBuilder().withFilterQuery("test:me", cached = false).build
        solrQuery shouldBe s"""$default_query_res_json,"fq":["{!cached=false }test:me"]}"""
      }

      it("should accept a filter query with cost options") {
        val solrQuery = new SolrQueryBuilder().withFilterQuery("test:me", cost = 100).build
        solrQuery shouldBe s"""$default_query_res_json,"fq":["{!cost=100 }test:me"]}"""
      }

      it("should accept a filter query with caching off and cost options") {
        val solrQuery = new SolrQueryBuilder().withFilterQuery("test:me", cached = false, cost = 100).build
        solrQuery shouldBe s"""$default_query_res_json,"fq":["{!cached=false cost=100 }test:me"]}"""
      }
    }


    describe("withCommit") {
      it("should accept a commit true") {
        val solrQuery = new SolrQueryBuilder().withCommit(true).build
        solrQuery shouldBe s"""$default_query_res_json,"commit":true}"""
      }
    }

    describe("withPaging") {
      it("should accept a paging = driver") {
        val solrQuery = new SolrQueryBuilder().withPaging("driver").build
        solrQuery shouldBe s"""$default_query_res_json,"paging":"driver"}"""
      }
    }


    describe("withStart") {
      it("should accept a start value") {
        val solrQuery = new SolrQueryBuilder().withStart(2).build
        solrQuery shouldBe s"""$default_query_res_json,"start":"2"}"""
      }
    }


    describe("withShardsFailover") {
      it("should accept shards.failover true") {
        val solrQuery = new SolrQueryBuilder().withShardsFailover(true).build
        solrQuery shouldBe s"""$default_query_res_json,"shards.failover":true}"""
      }
    }

    describe("withShardsTolerant") {
      it("should accept shards.tolerant true") {
        val solrQuery = new SolrQueryBuilder().withShardsTolerant(true).build
        solrQuery shouldBe s"""$default_query_res_json,"shards.tolerant":true}"""
      }
    }

    describe("withDistribSinglePass") {
      it("should accept distrib.singlePass true") {
        val solrQuery = new SolrQueryBuilder().withDistribSinglePass(true).build
        solrQuery shouldBe s"""$default_query_res_json,"distrib.singlePass":true}"""
      }
    }


    describe("withTz") {
      it("should accept Tz") {
        val solrQuery = new SolrQueryBuilder().withTz("Americas/San_Francisco").build
        solrQuery shouldBe s"""$default_query_res_json,"tz":"Americas/San_Francisco"}"""
      }
    }

    describe("withRouteRange") {
      it("should accept Tz") {
        val solrQuery = new SolrQueryBuilder().withRouteRange(List("test", "me")).build
        solrQuery shouldBe s"""$default_query_res_json,"route.range":"test,me"}"""
      }
    }


    describe("withRoutePartition") {
      it("should accept a single Route partition") {
        val solrQuery = new SolrQueryBuilder().withRoutePartition(List("test", "me")).build
        solrQuery shouldBe s"""$default_query_res_json,"route.partition":["test|me"]}"""
      }

      it("should accept a multiple Route partitions") {
        val solrQuery = new SolrQueryBuilder().withRoutePartition(Seq(List("test", "me"), List("this", "yes"))).build
        solrQuery shouldBe s"""$default_query_res_json,"route.partition":["test|me","this|yes"]}"""
      }
    }


    describe("chaining params") {
      it("should accept multiple options") {
        val solrQuery = new SolrQueryBuilder().withRouteRange(List("test", "me")).withFilterQuery("field:test").build
        solrQuery shouldBe s"""$default_query_res_json,"fq":["field:test"],"route.range":"test,me"}"""
      }
    }
  }


  describe("HTTP") {

    val default_query_res = """?q=*%3A*"""

    describe("withSort") {
      it("should accept a field and order") {
        val solrQuery = new SolrQueryBuilder().withSort("field", "asc").buildHttp
        solrQuery shouldBe s"""$default_query_res&sort=field+asc"""
      }

      it("should accept multiple fields and orders") {
        val solrQuery = new SolrQueryBuilder().withSort("field", "asc").withSort("field2", "desc").buildHttp
        solrQuery shouldBe s"""$default_query_res&sort=field+asc%2Cfield2+desc"""
      }

      it("should accept a map of fields and orders") {
        val solrQuery = new SolrQueryBuilder().withSort(Map("field" -> "asc", "field2" -> "desc")).buildHttp
        solrQuery shouldBe s"""$default_query_res&sort=field+asc%2Cfield2+desc"""
      }

    }


    describe("withLimit") {
      it("should accept a limit") {
        val solrQuery = new SolrQueryBuilder().withLimit(100).buildHttp
        solrQuery shouldBe s"""$default_query_res&limit=100"""
      }
    }

    describe("withQuery") {
      it("should accept a custom query") {
        val solrQuery = new SolrQueryBuilder().withQuery("test:me").buildHttp
        solrQuery shouldBe s"""?q=test%3Ame"""
      }
    }

    describe("withQueryName") {
      it("should accept a custom query") {
        val solrQuery = new SolrQueryBuilder().withQueryName("test").buildHttp
        solrQuery shouldBe s"""$default_query_res&query.name=test"""
      }
    }


    describe("withFilterQuery") {
      it("should accept a filter query with no options") {
        val solrQuery = new SolrQueryBuilder().withFilterQuery("test:me").buildHttp
        solrQuery shouldBe s"""$default_query_res&fq=test%3Ame"""
      }

      it("should accept multiple filter queries with no options") {
        val solrQuery = new SolrQueryBuilder().withFilterQuery("test:me").withFilterQuery("test:me").buildHttp
        solrQuery shouldBe s"""$default_query_res&fq=test%3Ame&fq=test%3Ame"""
      }

      it("should accept a filter query with caching off options") {
        val solrQuery = new SolrQueryBuilder().withFilterQuery("test:me", cached = false).buildHttp
        solrQuery shouldBe s"""$default_query_res&fq=%7B%21cached%3Dfalse+%7Dtest%3Ame"""
      }

      it("should accept a filter query with cost options") {
        val solrQuery = new SolrQueryBuilder().withFilterQuery("test:me", cost = 100).buildHttp
        solrQuery shouldBe s"""$default_query_res&fq=%7B%21cost%3D100+%7Dtest%3Ame"""
      }

      it("should accept a filter query with caching off and cost options") {
        val solrQuery = new SolrQueryBuilder().withFilterQuery("test:me", cached = false, cost = 100).buildHttp
        solrQuery shouldBe s"""$default_query_res&fq=%7B%21cached%3Dfalse+cost%3D100+%7Dtest%3Ame"""
      }
    }


    describe("withCommit") {
      it("should accept a commit true") {
        val solrQuery = new SolrQueryBuilder().withCommit(true).buildHttp
        solrQuery shouldBe s"""$default_query_res&commit=true"""
      }
    }

    describe("withPaging") {
      it("should ignore a paging = driver") {
        val solrQuery = new SolrQueryBuilder().withPaging("driver").buildHttp
        solrQuery shouldBe s"""$default_query_res"""
      }
    }


    describe("withStart") {
      it("should accept a start value") {
        val solrQuery = new SolrQueryBuilder().withStart(2).buildHttp
        solrQuery shouldBe s"""$default_query_res&start=2"""
      }
    }


    describe("withShardsFailover") {
      it("should accept shards.failover true") {
        val solrQuery = new SolrQueryBuilder().withShardsFailover(true).buildHttp
        solrQuery shouldBe s"""$default_query_res&shards.failover=true"""
      }
    }

    describe("withShardsTolerant") {
      it("should accept shards.tolerant true") {
        val solrQuery = new SolrQueryBuilder().withShardsTolerant(true).buildHttp
        solrQuery shouldBe s"""$default_query_res&shards.tolerant=true"""
      }
    }

    describe("withDistribSinglePass") {
      it("should accept distrib.singlePass true") {
        val solrQuery = new SolrQueryBuilder().withDistribSinglePass(true).buildHttp
        solrQuery shouldBe s"""$default_query_res&distrib.singlePass=true"""
      }
    }


    describe("withTz") {
      it("should accept Tz") {
        val solrQuery = new SolrQueryBuilder().withTz("Americas/San_Francisco").buildHttp
        solrQuery shouldBe s"""$default_query_res&tz=Americas%2FSan_Francisco"""
      }
    }

    describe("withRouteRange") {
      it("should accept Tz") {
        val solrQuery = new SolrQueryBuilder().withRouteRange(List("test", "me")).buildHttp
        solrQuery shouldBe s"""$default_query_res&route.range=test%2Cme"""
      }
    }


    describe("withRoutePartition") {
      it("should accept a single Route partition") {
        val solrQuery = new SolrQueryBuilder().withRoutePartition(List("test", "me")).buildHttp
        solrQuery shouldBe s"""$default_query_res&route.partition=test%7Cme"""
      }

      it("should accept a multiple Route partitions") {
        val solrQuery = new SolrQueryBuilder().withRoutePartition(Seq(List("test", "me"), List("this", "yes"))).buildHttp
        solrQuery shouldBe s"""$default_query_res&route.partition=test%7Cme%2Cthis%7Cyes"""
      }
    }


    describe("chaining params") {
      it("should accept multiple options") {
        val solrQuery = new SolrQueryBuilder().withRouteRange(List("test", "me")).withFilterQuery("field:test").buildHttp
        solrQuery shouldBe s"""$default_query_res&fq=field%3Atest&route.range=test%2Cme"""
      }
    }
  }
}
