package com.datastax.gatling.stress.helpers

import io.gatling.core.Predef._

object SessionHelper {

  /**
    * Functions fo
    */
  object strType {

    /**
      * Validate a key in session of string type is present and length is > 0
      *
      * @param session
      * @param sessionKey
      * @return
      */
    def keyValid(session: Session, sessionKey: String): Boolean = {
      session(sessionKey).as[String].length > 0
    }


    /**
      * Check if key is defined in Session
      *
      * @param session
      * @param sessionKey
      * @return
      */
    def keyDefined(session: Session, sessionKey: String): Boolean = {
      session(sessionKey).asOption[String].isDefined
    }


    /**
      * Validate Two Session Keys Match in value
      *
      * @param session
      * @param sessionKeyOne
      * @param sessionKeyTwo
      * @return
      */
    def keysMatch(session: Session, sessionKeyOne: String, sessionKeyTwo: String): Boolean = {

      if (keyValid(session, sessionKeyOne) && keyValid(session, sessionKeyTwo)) {
        if (session(sessionKeyOne).toString.eq(session(sessionKeyTwo).toString)) {
          return true
        }
      }

      false
    }


  }


}
