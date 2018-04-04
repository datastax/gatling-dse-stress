package com.datastax.gatling.stress.helpers

object ConfigHelpers {

  implicit class Strings(val string: String) extends AnyVal {

    def trimToOption = string.trim match {
      case "" => None
      case s => Some(s)
    }

  }

}
