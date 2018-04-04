package com.datastax.gatling.stress.helpers

import java.net.URLEncoder

import org.json4s.native.Serialization
import org.json4s.{DefaultFormats, FieldSerializer, Formats}

import scala.collection.mutable.ListBuffer


class SolrQueryBuilder {

  private val fqs = ListBuffer[String]()

  private val q: StringBuilder = new StringBuilder

  private var start: Option[String] = None

  private val sort: ListBuffer[String] = new ListBuffer[String]()

  private var limit: Option[String] = None

  private var queryName: Option[String] = None

  private var routePartition: Option[List[String]] = None

  private var tz: Option[String] = None

  private var paging: Option[String] = None

  private var shardsFailover: Option[Boolean] = None

  private var shardsTolerant: Option[Boolean] = None

  private var distribSinglePass: Option[Boolean] = None

  private var routeRange: Option[List[String]] = None

  private var commit: Option[Boolean] = None

  private var fields: Option[String] = None

  private val renames = FieldSerializer[SolrQuery](
    FieldSerializer.renameTo("queryName", "query.name")
        orElse FieldSerializer.renameTo("routeRange", "route.range")
        orElse FieldSerializer.renameTo("routePartition", "route.partition")
        orElse FieldSerializer.renameTo("shardsTolerant", "shards.tolerant")
        orElse FieldSerializer.renameTo("shardsFailover", "shards.failover")
        orElse FieldSerializer.renameTo("distribSinglePass", "distrib.singlePass")
  )

  implicit val format: Formats = DefaultFormats + renames


  def withFilterQuery(query: String, cached: Boolean = true, cost: Int = -1) = {
    val fqString = new StringBuilder

    if (!cached || cost > 0) {
      fqString.append("{!")
      if (!cached) fqString.append("cached=false ")
      if (cost > 0) fqString.append(s"cost=${cost.toString} ")
      fqString.append("}")
    }

    fqs.append(fqString.append(query).toString)
    this
  }


  def withQuery(query: String) = {
    q.append(query)
    this
  }

  def withQueryName(name: String) = {
    queryName = Some(name)
    this
  }


  def withSort(field: String, order: String) = {
    sort.append(s"$field $order")
    this
  }

  def withSort(sorts: Map[String, String]) = {
    sort.append(sorts.map(_.productIterator.mkString(" ")).mkString(","))
    this
  }

  def withStart(strt: Int) = {
    start = Some(strt.toString)
    this
  }

  def withLimit(lim: Int) = {
    limit = Some(lim.toString)
    this
  }

  def withTz(timezone: String) = {
    tz = Some(timezone)
    this
  }

  def withRoutePartition(partitionKeys: List[String]) = {
    routePartition = Some(List(partitionKeys.mkString("|")))
    this
  }

  def withRoutePartition(partitionKeys: Seq[List[String]]) = {
    routePartition = Some(partitionKeys.map(_.mkString("|")).toList)
    this
  }

  def withRouteRange(ranges: List[String]) = {
    routeRange = Some(ranges)
    this
  }

  def withPaging(pg: String) = {
    if (pg == "driver") {
      paging = Some(pg)
    }
    this
  }

  def withCommit(opt: Boolean) = {
    commit = Some(opt)
    this
  }

  def withShardsFailover(opt: Boolean) = {
    shardsFailover = Some(opt)
    this
  }

  def withShardsTolerant(opt: Boolean) = {
    shardsTolerant = Some(opt)
    this
  }

  def withDistribSinglePass(opt: Boolean) = {
    distribSinglePass = Some(opt)
    this
  }


  def withFields(fieldList: List[String]) = {
    fields = Some(fieldList.mkString(","))
    this
  }

  /**
    * Alias of buildCql for backward compatibility
    *
    * @return
    */
  def build = buildCql

  /**
    * Build a CQL JSON compatible query
    *
    * @return
    */
  def buildCql = {
    Serialization.write(
      SolrQuery(
        q = if (q.nonEmpty) q.toString else "*:*",
        fq = if (fqs.nonEmpty) Some(fqs.toList) else None,
        sort = if (sort.nonEmpty) Some(sort.mkString(",")) else None,
        start = if (start.nonEmpty) start else None,
        routePartition = if (routePartition.nonEmpty) routePartition else None,
        tz = if (tz.nonEmpty) tz else None,
        distribSinglePass = if (distribSinglePass.nonEmpty) distribSinglePass else None,
        shardsFailover = if (shardsFailover.nonEmpty) shardsFailover else None,
        shardsTolerant = if (shardsTolerant.nonEmpty) shardsTolerant else None,
        commit = if (commit.nonEmpty) commit else None,
        routeRange = if (routeRange.nonEmpty) Some(routeRange.get.mkString(",")) else None,
        queryName = if (queryName.nonEmpty) queryName else None,
        paging = if (paging.nonEmpty) paging else None,
        limit = if (limit.nonEmpty) limit else None
      )
    )
  }

  /**
    * Build a Solr HTTP compatible query
    *
    * @return
    */
  def buildHttp = {

    val httpQueryParams = new StringBuilder

    if (q.nonEmpty) {
      httpQueryParams.append(s"q=${encodeString(q.toString)}")
    } else {
      httpQueryParams.append(s"q=${encodeString("*:*")}")
    }

    if (fqs.nonEmpty) fqs.foreach(s => httpQueryParams.append(s"&fq=${encodeString(s)}"))
    if (sort.nonEmpty) httpQueryParams.append("&sort=" + encodeString(sort.mkString(",")))
    if (start.nonEmpty) httpQueryParams.append(s"&start=${start.get}")
    if (limit.nonEmpty) httpQueryParams.append(s"&limit=${limit.get}")
    if (queryName.nonEmpty) httpQueryParams.append(s"&query.name=${queryName.get}")
    if (commit.nonEmpty) httpQueryParams.append(s"&commit=${commit.get}")
    if (fields.nonEmpty) httpQueryParams.append(s"&fields=${fields.get}")
    if (routePartition.nonEmpty) httpQueryParams.append(s"&route.partition=${encodeString(routePartition.get.mkString(","))}")
    if (routeRange.nonEmpty) httpQueryParams.append(s"&route.range=${encodeString(routeRange.get.mkString(","))}")
    if (tz.nonEmpty) httpQueryParams.append(s"&tz=${encodeString(tz.get)}")
    if (distribSinglePass.nonEmpty) httpQueryParams.append(s"&distrib.singlePass=${distribSinglePass.get}")
    if (shardsFailover.nonEmpty) httpQueryParams.append(s"&shards.failover=${shardsFailover.get}")
    if (shardsTolerant.nonEmpty) httpQueryParams.append(s"&shards.tolerant=${shardsTolerant.get}")

    "?" + httpQueryParams.toString()
  }

  def encodeString(str: String) = {
    URLEncoder.encode(str, "UTF-8")
  }

  case class SolrQuery(q: String = "*:*",
                       fq: Option[List[String]] = None,
                       sort: Option[String] = None,
                       facet: Option[String] = None,
                       start: Option[String] = None,
                       tz: Option[String] = None,
                       paging: Option[String] = None,
                       distribSinglePass: Option[Boolean] = None,
                       shardsFailover: Option[Boolean] = None,
                       shardsTolerant: Option[Boolean] = None,
                       commit: Option[Boolean] = None,
                       routePartition: Option[List[String]] = None,
                       routeRange: Option[String] = None,
                       queryName: Option[String] = None,
                       limit: Option[String] = None
                      )

}

