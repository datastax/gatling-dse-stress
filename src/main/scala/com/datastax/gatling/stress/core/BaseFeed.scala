package com.datastax.gatling.stress.core

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

import com.datastax.driver.core.utils.UUIDs
import com.datastax.gatling.stress.libs.fakedata.JsonDataGenerator
import com.github.javafaker.Faker
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.math3.distribution.EnumeratedDistribution
import org.apache.commons.math3.util.Pair
import org.json4s._
import org.json4s.native.Serialization

import scala.util.Random


abstract class BaseFeed extends LazyLogging {

  protected lazy val faker: Faker = new Faker
  protected lazy val jsonDataGenerator: JsonDataGenerator = new JsonDataGenerator

  protected val idTypes = Array("PRODUCT_ID", "UPC", "ITEM_ID")
  protected val productClassTypes = Array("VARIANT", "REGULAR", "BVSHELL")
  protected val status = Array("ACTIVE", "INACTIVE")
  protected val publishStatus = Array("PUBLISHED", "DRAFT", "PENDING", "FUTURE", "PRIVATE", "DELETED", "HIDDEN")
  protected val sellerType = Array("EXTERNAL", "INTERNAL")
  protected val bool = Array(true, false)
  protected val location = Array("ONLINE", "STORE")
  protected val availStatus = Array("NOT_AVAILABLE", "AVAILABLE", "BACK_ORDER")
  protected val shipMethod = Array("GROUND", "EXPEDITED", "NEXT_DAY", "TWO_DAY", "STORE", "STANDARD")
  protected val subsystemName = Array("CATALOG")

  implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

  /**
    * Get Random Value from Array
    *
    * @param array
    * @tparam T
    * @return
    */
  protected def getRandom[T](array: Array[T]): T = {
    getRandom(array.toSeq)
  }

  /**
    * Get Random value from List
    *
    * @param list
    * @tparam T
    * @return
    */
  protected def getRandom[T](list: List[T]): T = {
    getRandom(list.toSeq)
  }

  /**
    * Get Random Value from Seq
    *
    * @param seq
    * @tparam T
    * @return
    */
  protected def getRandom[T](seq: Seq[T]): T = {
    seq(Random.nextInt(seq.length))
  }


  /**
    * Get Random Type4 Uuid
    *
    * @return
    */
  protected def getUuid: UUID = {
    UUIDs.random()
  }

  /**
    * Get Current Timmuid
    *
    * @return
    */
  protected def getTimeUuid: UUID = {
    UUIDs.timeBased()
  }

  /**
    * Strip Char from String
    *
    * @param str
    * @param replace
    * @param rWith
    * @return
    */
  protected def stripChar(str: String, replace: String, rWith: String): String = {
    str.replace(replace, rWith)
  }

  /**
    * Get Price alias
    *
    * @return
    */
  protected def getPrice: Float = {
    faker.commerce.price.toFloat
  }

  /**
    * Get a random Epoch timestamp
    *
    * @deprecated
    * @return
    */
  protected def getRandomEpoch: Timestamp = {
    this.getRandomTimestamp()
  }

  /**
    * Get a random timestamp between 2012
    *
    * @param startTs
    * @param endTs
    * @return
    */
  protected def getRandomTimestamp(startTs: String = null, endTs: String = null): Timestamp = {

    val startTime = Timestamp.valueOf(Option(startTs).getOrElse("2012-01-01 00:00:00")).getTime
    val endTime = if (endTs == null) {
      Timestamp.from(Instant.now).getTime
    } else {
      Timestamp.valueOf(Option(endTs).getOrElse("2017-01-01 00:00:00")).getTime
    }

    val time: Long = (startTime + (Math.random() * (endTime - startTime + 1))).toLong
    new Timestamp(time)
  }

  /**
    * Get Current Timestamp
    *
    * @return
    */
  protected def getCurrentTimestamp: Timestamp = {
    val time: Long = System.currentTimeMillis()
    new Timestamp(time)
  }


  /**
    * Generate Upc Code
    *
    * @return
    */
  protected def getUpc: String = {
    faker.numerify("############")
  }

  /**
    * Generate long ID String
    *
    * @return
    */
  protected def getIdString: String = {
    getLongStrId
  }

  /**
    * Generate short ID String
    *
    * @return
    */
  protected def getShortIdString: String = {
    faker.bothify("#??###?#?")
  }

  /**
    * Generate long ID String
    *
    * @return
    */
  protected def getLongStrId: String = {
    faker.bothify("#??###??###?#??#?#?##??#?####?#?")
  }

  /**
    * Get Random ID String
    *
    * @return
    */
  protected def getRandomIdStr: String = {
    getRandom(Array(
      getShortIdString,
      getLongStrId,
      getUpc.toString
    ))
  }

  /**
    * Convert to Json String
    *
    * @param anyRef
    * @return
    */
  protected def getJsonString(anyRef: AnyRef): String = {
    Serialization.write(anyRef)
  }


  /**
    * Create a weighted bias list and get values
    *
    * @param items
    */
  class WeightedRandomGenerator(var items: List[(Any, Double)]) {

    private val distMap = new java.util.ArrayList[Pair[Any, java.lang.Double]]()

    for (i <- items) {
      distMap.add(new Pair(i._1, i._2))
    }

    private val dist = new EnumeratedDistribution(distMap)

    def getItem = {
      dist.sample()
    }
  }

}
