name := "spark-job"
version := "1.0.0"

scalaVersion := "2.12.18"

val sparkVersion = "3.5.4"

// Scala compiler settings
scalacOptions ++= Seq(
  "-encoding", "utf8",
  "-feature",
  "-unchecked",
  "-deprecation",
  "-release", "17"
)

// Java compiler settings
javacOptions ++= Seq(
  "-source", "17",
  "-target", "17"
)

// Dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-kubernetes" % sparkVersion % "provided",

  "com.clickhouse" % "clickhouse-jdbc" % "0.9.2",
  "com.typesafe" % "config" % "1.4.3",

  // Hadoop AWS support
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4" % "provided",
  "org.apache.hadoop" % "hadoop-common" % "3.3.4" % "provided"
)

// Testing
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.17" % Test

Compile / run / fork := true
Test / fork := true
Test / parallelExecution := false
