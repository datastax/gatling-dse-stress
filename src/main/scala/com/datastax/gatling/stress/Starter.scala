package com.datastax.gatling.stress

import java.net.URISyntaxException

import com.datastax.gatling.stress.libs.AppConfig
import com.datastax.gatling.stress.utils.{GatlingRunnerConfig, SimulationFinder}
import com.typesafe.config.ConfigRenderOptions
import io.gatling.app.Gatling
import io.gatling.core.config.GatlingPropertiesBuilder

import java.nio.file.FileSystems

/**
  * Start Class for All Options
  */
object Starter {

  def main(args: Array[String]): Unit = {

    // Workaround for https://bugs.openjdk.java.net/browse/JDK-8194653
    FileSystems.getDefault
    val parser = new scopt.OptionParser[GatlingRunnerConfig]("") {

      override def showUsageOnError = true

      head("Gatling DSE")

      cmd("run").action((_, c) => c.copy(mode = "run")).text("Run Simulation").children(

        arg[String]("<simulation>").required().action((x, c) => c.copy(simulation = x))
            .text("Simulation class to run. Can be full path or just class name. *REQUIRED*"),

        opt[Unit]("no-reports").abbr("no").action((_, c) =>
          c.copy(noReports = true)).text("Runs simulation but does not generate reports."),

        opt[String]("reports-only").abbr("ro").valueName("<folder>").action((x, c) => c.copy(reportsOnly = x))
            .text("Generates the reports for the simulation log file located in <folderName>."),

        opt[String]("results-folder").abbr("rf").valueName("<path>").action((x, c) => c.copy(reportsOnly = x))
            .text("Uses <path> as the folder where results are stored."),

        opt[String]("run-description").abbr("rd").valueName("<description>").action((x, c) => c.copy(reportsOnly = x))
            .text("A short <description> of the run to include in the report.")
      )

      cmd("listSims").action((_, c) => c.copy(mode = "listsims")).text("\tList all simulations available.")
      cmd("showConf").action((_, c) => c.copy(mode = "showconf")).text("\tShow current configurations.").children(
        arg[String]("<section>").optional().action((x, c) => c.copy(confShow = x))
            .text("Show section of conf including: all, general, cassandra, simulations, gatling, <custom>")
      )

      cmd("stressVersion").action((_, c) => c.copy(mode = "stressversion"))
          .text("\tGet version of Gatling DSE Stress framework.")


      note("\nTo override default configs use JAVA_OPTS=\"-Dconfig.file=new.conf\" ./gatling-dse-sims run {sim}  " +
          "or JAVA_OPTS=\"-Dcassandra.auth.username=user\" ./gatling-dse-sims run {sim}\n")
    }

    parser.parse(args, GatlingRunnerConfig()) match {
      case Some(config) =>
        config.mode.toLowerCase match {
          case "listsims" =>
            listSims()
          case "showconf" =>
            showConf(config)
          case "stressversion" =>
            stressVersion()
          case "run" =>
            run(config)
          case _ =>
            parser.showUsage()
        }
      case None =>

    }
  }

  private def stressVersion() = {
    val stream = getClass.getResourceAsStream("/version.txt")
    val lines = scala.io.Source.fromInputStream( stream ).getLines
    lines.foreach(println)
  }

  private def listSims() = {
    println(SimulationFinder.getSimList)
  }

  @throws[URISyntaxException]
  private def showConf(config: GatlingRunnerConfig) = {
    val conf = new AppConfig().loadConfig()
    val options = ConfigRenderOptions.defaults.setOriginComments(false).setComments(false)

    val sectionList = List("general", "cassandra", "simulations", "gatling")

    config.confShow.toLowerCase match {
      case "all" =>
        sectionList.foreach { x =>
          println(x + ":")
          println(conf.getConfig(x).root.render(options))
        }
      case custom =>
        if (conf.hasPath(custom)) {
          println(custom + ":")
          println(conf.getConfig(custom).root.render(options))
        }
    }
  }


  private def run(config: GatlingRunnerConfig) = {

    val simList = SimulationFinder.findSimulation(config.simulation)

    if (simList.length < 1) {
      println("ERROR: No simulations found with name given. Please use listSims to list all sims.")
      System.exit(1)
    }

    if (simList.length > 1) {
      val output = new StringBuilder
      output.append("ERROR: More than one simulation found. Please use a more specific name. Matching simulations:\n")

      for (a <- simList) {
        output.append("\t - " + a + "\n")
      }

      print(output.toString)
      System.exit(1)
    }

    val props = new GatlingPropertiesBuilder
    props.mute()

    if (config.noReports) {
      props.noReports()
    }

    if (!config.resultsFolder.isEmpty) {
      props.resultsDirectory(config.resultsFolder)
    }

    if (!config.reportsOnly.isEmpty) {
      props.reportsOnly(config.reportsOnly)
    }

    if (!config.runDescription.isEmpty) {
      props.runDescription(config.runDescription)
    }

    props.simulationClass(simList.head)
    try {
      val res = Gatling.fromMap(props.build)
      System.exit(res)
    }
    catch{
      case e: Exception =>
        print(e.printStackTrace())
        System.exit(1)

    }

  }
}
