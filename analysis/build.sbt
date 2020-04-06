name := "analysis"

version := "0.1"

scalaVersion := "2.11.8"
//libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.3" % Runtime
libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.9.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.1.0" % "provided"
libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.1"
