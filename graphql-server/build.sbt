ThisBuild / scalaVersion := "2.13.18"

libraryDependencies ++= Seq(
  // Akka
  "com.typesafe.akka" %% "akka-actor-typed" % "2.8.5",
  "com.typesafe.akka" %% "akka-stream"      % "2.8.5",
  "com.typesafe.akka" %% "akka-http"        % "10.5.3",

  // GraphQL
  "org.sangria-graphql" %% "sangria" % "4.0.2",
  "org.sangria-graphql" %% "sangria-circe" % "1.3.2",

  // JSON
  "io.circe" %% "circe-core"    % "0.14.6",
  "io.circe" %% "circe-parser" % "0.14.6",

  // Database
  "org.postgresql" % "postgresql" % "42.7.3",
  "com.zaxxer" % "HikariCP" % "5.1.0",

  // Config
  "com.typesafe" % "config" % "1.4.3"
)

