name := "gatling-dse-stress"
organization := "com.datastax.gatling.stress"
releaseUseGlobalVersion := false
scalaVersion := "2.12.5"

scalacOptions ++= Seq("-target:jvm-1.8", "-Ybreak-cycles")

resolvers ++= Seq(
  Resolver.mavenLocal,
  Resolver.mavenCentral,
  Resolver.jcenterRepo,
  "datastax-release" at "https://datastax.jfrog.io/datastax/repo"
)

libraryDependencies ++= Seq(
  // Formerly in lib directory
  "com.datastax.gatling.plugin" %% "gatling-dse-plugin" % "1.3.1",
  "com.github.javafaker" % "javafaker" % "0.14",

  // From build.gradle
  "com.github.scopt" %% "scopt" % "3.7.0",
  "org.json4s" %% "json4s-native" % "3.5.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "org.apache.commons" % "commons-math3" % "3.6.1",
  "org.apache.commons" % "commons-lang3" % "3.7",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe" % "config" % "1.3.2",
  "io.gatling.highcharts" % "gatling-charts-highcharts" % "2.3.0" % Provided,

  // Testing resources
  "org.scalatest" %% "scalatest" % "3.0.4" % Test,
  "junit" % "junit" % "4.12" % Test,
  "org.cassandraunit" % "cassandra-unit" % "3.3.0.2" % Test,
  "org.pegdown" % "pegdown" % "1.6.0" % Test,
  "org.easymock" % "easymock" % "3.5.1" % Test,
  "org.fusesource" % "sigar" % "1.6.4" % Test
)

headerLicense := Some(HeaderLicense.Custom(
  """|Copyright (c) 2018 Datastax Inc.
     |
     |This software can be used solely with DataStax products. Please consult the file LICENSE.md."""
     .stripMargin
))

//
// Several integration tests start an embedded C* server.
// When the SBT shell is used and the JVM is not forked, MBean conflicts happen at the second test suite execution
// Make sure to fork the JVM so that every test suite starts from a clean state
//
Test / fork := true

//
// When building an uberjar, discard the dependencies duplicate files that are under META-INF
//
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

//
//
//
mainClass in assembly := Some("com.datastax.gatling.stress.Starter")

//
// Releases should reuse credentials from other build systems
//
// For Jenkins triggered releases, find them in the file denoted by the environment variable MAVEN_USER_SETTINGS_FILE
// If it is missing, find them in ~/.m2/settings.xml
//
val settingsXml = sys.env.getOrElse("MAVEN_USER_SETTINGS_FILE", System.getProperty("user.home") + "/.m2/settings.xml")
val mavenSettings = scala.xml.XML.loadFile(settingsXml)
val artifactory = mavenSettings \ "servers" \ "server" filter { node => (node \ "id").text == "artifactory" }
publishTo := {
  if (isSnapshot.value) {
    Some("Artifactory Realm" at "http://datastax.jfrog.io/datastax/datastax-public-snapshots-local;" +
      "build.timestamp=" + new java.util.Date().getTime)
  } else {
    Some("Artifactory Realm" at "http://datastax.jfrog.io/datastax/datastax-public-releases-local")
  }
}
credentials += Credentials(
  "Artifactory Realm",
  "datastax.jfrog.io",
  (artifactory \ "username").text,
  (artifactory \ "password").text)
