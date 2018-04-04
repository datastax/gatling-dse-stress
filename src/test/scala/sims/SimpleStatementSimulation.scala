/**
  * The MIT License (MIT)
  *
  * Original work Copyright (c) 2015 Mikhail Stepura
  * Modified work Copyright 2016 Brad Vernon
  *
  * Permission is hereby granted, free of charge, to any person obtaining a copy of
  * this software and associated documentation files (the "Software"), to deal in
  * the Software without restriction, including without limitation the rights to
  * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
  * the Software, and to permit persons to whom the Software is furnished to do so,
  * subject to the following conditions:
  *
  * The above copyright notice and this permission notice shall be included in all
  * copies or substantial portions of the Software.
  *
  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
  * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
  * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
  * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
  * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
  */
package sims

import com.datastax.gatling.stress.core.BaseSimulation
import io.gatling.core.Predef._
import com.datastax.gatling.plugin.CqlPredef._

import scala.concurrent.duration.DurationInt

class SimpleStatementSimulation extends BaseSimulation {


  cass.getBuilder.withPort(9042)


  val table_name = "test_table_simple"

//  createTestKeyspace
//  createTable

  //val cqlConfig = cql.session(cass.getSession) //Initialize Gatling DSL with your session

  val insertId = 1
  val insertStr = "one"

  val testKeyspace = "test"

  val simpleStatementInsert = s"""INSERT INTO $testKeyspace.$table_name (id, str) VALUES ($insertId, '$insertStr')"""
  val simpleStatementSelect = s"""SELECT * FROM $testKeyspace.$table_name WHERE id = $insertId"""

  val insertCql = cql("Simple Insert Statement")
      .executeCql(simpleStatementInsert)

  val selectCql = cql("Simple Select Statement")
      .executeCql(simpleStatementSelect)

  val scn = scenario("SimpleStatement")
      .exec(insertCql
          .check(exhausted is true)
          .check(rowCount is 0) // "normal" INSERTs don't return anything
      )
      .pause(1.seconds)

      .exec(selectCql
          .check(rowCount is 1)
          .check(columnValue("str") is insertStr)
      )

  setUp(
    scn.inject(constantUsersPerSec(1) during 1.seconds).protocols(cqlProtocol)
  ).assertions(
    global.failedRequests.count.is(0)
  )



  def createTable = {
    val table =
      s"""
      CREATE TABLE IF NOT EXISTS $testKeyspace.$table_name (
      id int,
      str text,
      PRIMARY KEY (id)
    );"""

    cass.getSession.execute(table)

    //session.execute(s"""TRUNCATE TABLE $testKeyspace.$table_name""")
  }
}
