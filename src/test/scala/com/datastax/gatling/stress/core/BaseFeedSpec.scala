package com.datastax.gatling.stress.core


import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

import com.datastax.gatling.stress.base.BaseSpec

class BaseFeedSpec extends BaseSpec {

  val baseFeed = new BaseFeedStub

  describe("getJsonString") {

    it("should encode an Array") {
      val json = baseFeed.getJsonString(Array("test", "me"))
      json shouldBe """["test","me"]"""
    }

    it("should encode a Map") {
      val json = baseFeed.getJsonString(Map("test" -> "me"))
      json shouldBe """{"test":"me"}"""
    }
  }

  describe("getTimeUuid") {
    it("should be a UUID type") {
      baseFeed.getTimeUuid shouldBe a[UUID]
    }
  }

  describe("getUuid") {
    it("should be a UUID type") {
      baseFeed.getUuid shouldBe a[UUID]
    }
  }

  describe("stripChar") {
    it("should be a correctly strip chars") {
      baseFeed.stripChar("tester", "er", "me") shouldBe "testme"
    }
  }

  describe("getTimestamp") {
    it("should be a timestamp type") {
      baseFeed.getCurrentTimestamp shouldBe a[java.util.Date]
    }
  }

  describe("getCurrentEpoch") {
    it("should be a timestamp type") {
      baseFeed.getCurrentTimestamp shouldBe a[java.util.Date]
    }
  }

  describe("getRandomTimestamp") {

    it("should be accept no values") {

      val time = baseFeed.getRandomTimestamp()
      time shouldBe a[java.util.Date]

      val startTime = Timestamp.valueOf("2012-01-01 00:00:00").getTime
      val endTime = Timestamp.valueOf("2017-01-01 00:00:00").getTime

//      time.getTime should (be >= startTime and be <= endTime)
      time.getTime should (be >= startTime)
    }


    it("should be accept startTime only") {

      val time = baseFeed.getRandomTimestamp("2014-01-01 00:00:00")
      time shouldBe a[java.util.Date]

      val startTime = Timestamp.valueOf("2014-01-01 00:00:00").getTime
      val endTime = Timestamp.from(Instant.now).getTime

//      time.getTime should (be >= startTime and be <= endTime)
      time.getTime should (be >= startTime)
    }


    it("should be accept values") {

      val time = baseFeed.getRandomTimestamp("2012-01-01 00:00:00", "2017-01-01 00:00:00")
      time shouldBe a[java.util.Date]

      val startTime = Timestamp.valueOf("2012-01-01 00:00:00").getTime
      val endTime = Timestamp.valueOf("2017-01-01 00:00:00").getTime

//      time.getTime should (be >= startTime and be <= endTime)
      time.getTime should (be >= startTime)
    }

  }


  describe("getRandom") {

    it("should accept array of strings") {
      val res = baseFeed.getRandom(Array("test", "me"))

      res shouldBe a[String]
    }

    it("should accept array of arrays") {
      val res = baseFeed.getRandom(Array(Array("test"), Array("me")))
      res shouldBe a[Array[_]]
    }

    it("should accept seq of strings") {
      val res = baseFeed.getRandom(Seq("test", "me"))
      res shouldBe a[String]
    }

    it("should accept list of strings") {
      val res = baseFeed.getRandom(List("test", "me"))
      res shouldBe a[String]
    }

  }

  describe("weightedRandomGenerator") {

    val list1 = List(("me", 50.0), ("you", 50.0))
    val list2 = List(("cat", 50.0), ("dog", 50.0))
    val list3 = List(("cat", 80.0), ("dog", 1.0))
    val list4 = List(("cat", 80.0), ("dog", 1.0), ("monkey", 0.0))

    it("should return one of the expected values") {
      val weightedRandom = new baseFeed.WeightedRandomGenerator(list1)
      weightedRandom.getItem should (be("me") or be("you"))
      weightedRandom.getItem should not(be("cat") or be("dog"))
    }

    it("should not collide with another instance") {
      val weightedRandom1 = new baseFeed.WeightedRandomGenerator(list1)
      val weightedRandom2 = new baseFeed.WeightedRandomGenerator(list2)

      weightedRandom1.getItem should (be("me") or be("you"))
      weightedRandom2.getItem should (be("cat") or be("dog"))
    }

    it("should accept values not up to 100 total") {
      val weightedRandom = new baseFeed.WeightedRandomGenerator(list4)
      val item = weightedRandom.getItem
      item should (be("cat") or be("dog"))
      item should not be "monkey"
    }
  }

}

/**
  * Stub
  */
class BaseFeedStub extends BaseFeed {

  override def getJsonString(anyRef: AnyRef) = {
    super.getJsonString(anyRef)
  }

  override def getTimeUuid = {
    super.getTimeUuid
  }

  override def getUuid = {
    super.getUuid
  }

  override def getCurrentTimestamp = {
    super.getCurrentTimestamp
  }

  override def getRandomEpoch = {
    super.getRandomEpoch
  }

  override def getRandomTimestamp(startTs: String = null, endTs: String = null) = {
    super.getRandomTimestamp(startTs, endTs)
  }

  override def getPrice = {
    super.getPrice
  }

  override def getUpc = {
    super.getUpc
  }

  override def getIdString = {
    super.getIdString
  }

  override def getShortIdString = {
    super.getShortIdString
  }

  override def getLongStrId = {
    super.getLongStrId
  }

  override def getRandomIdStr = {
    super.getRandomIdStr
  }

  override def getRandom[T](array: Array[T]) = {
    super.getRandom[T](array)
  }

  override def getRandom[T](list: List[T]) = {
    super.getRandom[T](list)
  }

  override def getRandom[T](seq: Seq[T]) = {
    super.getRandom[T](seq)
  }

  override def stripChar(str: String, replace: String, rWith: String) = {
    super.stripChar(str, replace, rWith)
  }

}

