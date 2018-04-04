package com.datastax.gatling.stress.libs

import com.datastax.gatling.stress.base.BaseSpec
import com.datastax.gatling.stress.libs.fakedata.JsonDataGenerator
import org.json4s._
import org.json4s.native.JsonMethods._

class JsonDataGeneratorTest extends BaseSpec {

  val jsonDataGenerator = new JsonDataGenerator

  describe("A 1Kb JSON") {
    it("should have size of ~ 1kb") {
      jsonDataGenerator.oneKb.length should be < 1050
      jsonDataGenerator.oneKb.length should be > 995
    }
  }

  describe("A 5Kb JSON") {
    it("should have size of ~ 5kb") {
      jsonDataGenerator.fiveKb.length should be < 6000
      jsonDataGenerator.fiveKb.length should be > 4500
    }
  }

  describe("A 10Kb JSON") {
    it("should have size of ~ 10kb") {
      jsonDataGenerator.tenKb.length should be < 12000
      jsonDataGenerator.tenKb.length should be > 9500
    }
  }

  describe("A 25Kb JSON") {
    it("should have size of ~ 25kb") {
      jsonDataGenerator.twentyFiveKb.length should be < 28000
      jsonDataGenerator.twentyFiveKb.length should be > 23500
    }
  }

  describe("A 55Kb JSON") {
    it("should have size of ~ 55kb") {
      jsonDataGenerator.fiftyFiveKb.length should be < 57000
      jsonDataGenerator.fiftyFiveKb.length should be > 54500
    }
  }


  describe("Generated JSON") {

    it("should be close to the size requested") {
      var gen = jsonDataGenerator.generate(1)
      gen.length should be < 1050
      gen.length should be > 980

      gen = jsonDataGenerator.generate(10)
      gen.length should be < 10500
      gen.length should be > 9800
    }

    it("should be valid JSON") {
      val gen = jsonDataGenerator.generate(5)
      parse(gen) shouldBe a[JValue]
    }
  }

}

