name := "spark-graph"
version := "1.0.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.4"
val graphFramesVersion = "0.8.3-spark3.5-s_2.12"

// ----------------------------
// Resolvers (Maven Central first)
// ----------------------------
resolvers ++= Seq(
  Resolver.mavenCentral,                                      // <- Maven Central first
  "Spark Packages Repo" at "https://repos.spark-packages.org/" // <- fallback
)

// ----------------------------
// Scala compiler settings
// ----------------------------
scalacOptions ++= Seq(
  "-encoding", "utf8",
  "-feature",
  "-unchecked",
  "-deprecation",
  "-release", "17"
)

// ----------------------------
// Dependencies
// ----------------------------
libraryDependencies ++= Seq(
  // Spark core
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",

  // GraphFrames - use the complete version string
  "graphframes" % "graphframes" % graphFramesVersion,

  // Config support
  "com.typesafe" % "config" % "1.4.3"
)

// ----------------------------
// Testing
// ----------------------------
libraryDependencies +=
  "org.scalatest" %% "scalatest" % "3.2.17" % Test

// ----------------------------
// Runtime settings
// ----------------------------
Compile / run / fork := true
Test / fork := true
Test / parallelExecution := false
