# Functional Data Programming Project

## Functional Data Programming Project made by Radu Cernaianu, Dona Chadid, Clément Lambling and Louis Potron

### Files Breakdown:

- droneSimulation -> Drone Simulation software
- consumerDroneDbDump -> Saves drone messages to the Database
- droneAlertHandler -> Sends alerts to the website
- projectReadCSV -> Sends line by line the historical data through Kafka
- dumpCsvToDb -> Saves the historical data from Kafka to mongoDB
- analysis -> Analysis of historical data using spark
- drone_alert_site -> website displaying the alerts

### Build file with all dependancies

```
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.4.0"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.3" % Runtime
libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.9.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.1.0" % "provided"
libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.1"


```