# Change Log

## [v1.2.2] - 2018-04-03
New:
- Update to gatling-dse-plugin [v1.2.0](https://github.com/datastax/gatling-dse-plugin/releases/tag/v1.2.0)
- Migration to SBT

## [v1.2.1] - 2017-11-19
New: 
- Update to gatling-dse-plugin [v1.1.1](https://github.com/datastax/gatling-dse-plugin/releases/tag/v1.1.1)
- Added HTTP Solr query string builder to SolrQueryBuilder
- Add runOnlyOnce to LoadGenerator and skip rampTime for load of 1 user
- Configuration for DseStress key values is now based on case class

Fixes:
- Correct issue in SolrQueryBuilder for routePartition usage 

## [v1.2.0] - 2017-10-13
New:
- Updated gatling-dse-plugin to [v1.1.0](https://github.com/datastax/gatling-dse-plugin/releases/tag/v1.1.0)
  - Includes DSE Driver 1.4.0, enhanced logging, additional checks and request option setting
- Added SolrQueryBuilder helper class to simplify creating CQL Solr Queries using JSON
    - Example: `val q = new SolrQueryBuilder().withQuery("first_name:Test_user").withLimit(1).build`
- Added support for `PER PARTITION` in FetchBaseData for use with C* 3.6+ (can disable via configs)
- Updated Faker library to 0.14-snapshot to include new data points and features no in maven release
- Updated command line help to include how to override config values

Fixes:
- Corrects issue in Gatling conf defaults that used the wrong path
- Added log event for rampToConstant load generator during startup

Breaking Changes:
- Requires Scala 2.12.x
- Requires Gatling 2.3+


## [v1.1.1] - 2017-06-23
New:
- Updated gatling-dse-plugin to [v1.0.2](https://github.com/datastax/gatling-dse-plugin/releases/tag/v1.0.2)
  - Includes changes to allow `None` value in Feed/Session to be unset in a prepared statement
  - Adds support for batch prepared statements
- Added support for setting graph name during cluster building if present in configs
- Added support for getting the version of this stress library in command line `stressVersion`
- Added support for using `Seq` with `runQueries` and `getRandom`
- Added support for auto configuring the graph name during connection to the cluster if `graphName` is set in conf
- Added support for disabling creation of keyspace by adding `enabled = false` to simulations `createKeyspace` section

Fixes/Changes:
- Limited `listSims` to only show classes that end in 'Simulation'


## [v1.1.0] - 2017-05-14
New:
- Updated gatling-dse-plugin to [v1.0.0](https://github.com/datastax/gatling-dse-plugin/releases/tag/v1.0.0)
- Overhauled Cassandra session creation to include better defaults and added ability to set custom session/cluster builder
- New method for running simulations and Utility commands
- Added createKeyspace() function to BaseAction class to create keyspaces based on config properties
- Added WeightedRandomGenerator to BaseFeed
- Added function in BaseAction to create Keyspace based on Conf section params

Fixes/Changes:
- Fixed issue with Util simList not finding children of simulations
- Moved to using LazyLogger library for default logging
- Updated default configurations to be more load testing ready
- Updated dependencies to be latest versions

Breaking Changes:
- Moved to a package base name `com.datastax.gatling.stress.*`
  - Existing usage will require replacing class import packages.  No classes have been renamed.
- Requires use of Gatling v2.2.5+
- `{jar} -s <simName>` has been replaced by `{jar} run <simName>`
- Utils class and command line usage has been replaced with top level commands `{jar} listSims|showConf`
- Base* class functions set to protected which may require updates to code if referencing in Simulation classes
- BaseSimulation.buildRampConstScenario function removed. Use LoadGenerator.* instead


## [v1.0.6] - 2016-11-08
New/Changes: 
- Update to [gatling-dse-plugin v0.0.5](https://github.com/datastax/gatling-dse-plugin/tree/v0.0.5) w/ Fluent Graph API support and fix for metrics
- Fixed null pointer error with LoadGenerator.constantToTotal() and moved to static class
- Changed all non-essential declarations in Base classes to lazy loading
- Changed Base classes to abstract classes

Breaking Changes:
- Gatling 2.2.2 binary is no longer included in the build jar, need to add in sub-project
- To access Cassandra session use cass.getSession() instead of cass.session
- execute() for CQL and Graph are deprecated as part of gatling-dse-plugin, use executeCql(), executePrepared(), etc.. instead


## [v1.0.5] - 2016-09-26
- Fixed issue with loadGenerator rampUpToConstant and rampUpToPercentage setup to be more linear over x seconds from 1 user
- Fixed logging to be set to debug by default for stress framework related events
- Tuned down config logging to trace and updated methods to be more compact
- Included the first steps in making a more complete unit testing


## [v1.0.4] - 2016-09-14
- Addition of a SessionHelper class for validating Gatling session params
- Addition of function to BaseActions to run an array of queries for table creation
- New Load Generation profile: constantToTotal
- Addition of config for CQL port and ability to override


## [v1.0.3] - 2016-08-30
- Fix for simulation listing
- Including new loadGeneration library for multiple load profiles


## [v1.0.2] - 2016-08-25
- Enable ability to override the default log levels using a command line param, see Read Me for more
- When fetching com.datastax.gatling.stress.base data from more than one table in same the token range found during first table is reused
- Fixes issue with Fetching com.datastax.gatling.stress.base data when multiple columns are used
- Fixes Config value logging

## [v1.0.1] - 2016-08-24
- Addition of a JSON generator enabling valid JSON string of x kilobytes be generated quickly
- Updated example project to include Actions using LWT and validation of response

## [v1.0.1] - 2016-08-23
- First release of Gatling DSE Stress com.datastax.gatling.stress.base project.


[Unreleased]: https://github.com/datastax/gatling-dse-stress/compare/v1.2.1...HEAD
[v1.2.1]: https://github.com/datastax/gatling-dse-stress/compare/v1.2.1...v1.2.0
[v1.2.0]: https://github.com/datastax/gatling-dse-stress/compare/v1.2.0...v1.1.1
[v1.1.1]: https://github.com/datastax/gatling-dse-stress/compare/v1.1.0...v1.0.1
[v1.1.0]: https://github.com/datastax/gatling-dse-stress/compare/v1.1.0...v1.0.6
[v1.0.6]: https://github.com/datastax/gatling-dse-stress/compare/v1.0.6...v1.0.5
[v1.0.5]: https://github.com/datastax/gatling-dse-stress/compare/v1.0.5...v1.0.4
[v1.0.4]: https://github.com/datastax/gatling-dse-plugin/compare/v0.0.4...v1.0.3
[v1.0.3]: https://github.com/datastax/gatling-dse-plugin/compare/v0.0.3...v1.0.2
[v1.0.2]: https://github.com/datastax/gatling-dse-plugin/compare/v0.0.2...v1.0.1
[v1.0.1]: https://github.com/datastax/gatling-dse-plugin/compare/v1.0.1...v1.0.0
[v1.0.0]: https://github.com/datastax/gatling-dse-plugin/compare/v1.0.0...v1.0.0
