Gatling DSE Stress
==============

## Building
To build a jar run `sbt assembly`.  The jar will be found in `target/scala-2.12/gatling-dse-stress-assembly-1.2.2.jar`


## Running a Simulation
First build the jar using `sbt` then run the jar with the path of the sim name `java -jar gatling-dse-stress-1.2.2.jar run {SimPath}` and example is `java -jar gatling-dse-sims-1.2.2.jar run examples.sims.cql.WriteOrderSimulation`


## Configuration
Project configs can be found in the `src/main/resources` the `application.conf` is the file to set the Simulation and Cassandra settings.  

During run you can override part or all of the application settings by using `-Dconfig.file={filePath}`.  If you want to override a single setting only just use the path of the config ie `-Dcassandra.hosts=127.0.0.1`.  This single setting can be used for any value in the `application.conf` file as well.


### Listing Available Sims in Jar
Run `java -jar gatling-dse-stress-1.2.2.jar listSims`


### Show Default Configurations
Run `java -jar gatling-dse-stress-1.2.2.jar showConf`


### Show gatling-dse-stress framework version
Run `java -jar gatling-dse-stress-1.2.2.jar stressVersion`


## Using In a New Project
See SimCatalog Project at [gatling-dse-simcatalog)](https://github.com/datastax/gatling-dse-simcatalog)


## Overriding Default Log Levels
The following log can have their levels be overriden using `-D{logName}={LEVEL}`:
- DEBUG Defaults
    - log.libs
    - log.actions
    - log.feeds
    - log.sims
    - log.utils
- WARN Defaults
    - log.cassandra (includes all Cassandra Driver events)
    - log.root (includes all of the above)


## Requirements
- Java 1.8+
- SBT 1.1.2
- [gatling-dse-plugin](https://github.com/datastax/gatling-dse-plugin) - (Included `build.sbt` dependencies).

Running `sbt assembly` will download all of the needed libraries including Scala to your local machine.

## Questions or Requests
Please use the [Issues section](https://github.com/datastax/gatling-dse-stress/issues) to add any questions on usage or requests

There is also a `#gatling-dse` Slack channel where questions can be asked.

## Contributions

This project was developed by Brad Vernon ([ibspoof](https://github.com/ibspoof)) and improved by the following contributors:

* Sebastien Bastard ([datazef](https://github.com/datazef))
