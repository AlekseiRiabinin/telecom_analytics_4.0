name := "spark-job"
version := "1.0.0"

scalaVersion := "2.13.17"

val sparkVersion = "4.0.1"

scalacOptions ++= Seq(
  "-encoding", "utf8",
  "-feature",
  "-unchecked", 
  "-deprecation",
  "-release:21"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-kubernetes" % sparkVersion % "provided",
  "com.clickhouse" % "clickhouse-jdbc" % "0.9.2",
  "com.typesafe" % "config" % "1.4.3"
)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.17" % Test

Compile / run / fork := true
Test / fork := true

Test / parallelExecution := false

javacOptions ++= Seq("-source", "21", "-target", "21")
