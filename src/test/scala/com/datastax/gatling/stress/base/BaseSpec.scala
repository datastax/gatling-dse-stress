package com.datastax.gatling.stress.base

import org.scalatest._
import org.scalatest.easymock.EasyMockSugar

abstract class BaseSpec extends FunSpec with EasyMockSugar with Matchers with BeforeAndAfter with BeforeAndAfterAll {


}