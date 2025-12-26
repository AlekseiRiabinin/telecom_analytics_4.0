package cre

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import cre.db.Database
import cre.graphql._

import scala.concurrent.ExecutionContextExecutor


object Main extends App {

  implicit val system: ActorSystem = ActorSystem("graphql-cre")
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  val config = system.settings.config

  val ds =
    Database.dataSource(
      config.getString("db.host"),
      config.getInt("db.port"),
      config.getString("db.name"),
      config.getString("db.user"),
      config.getString("db.password")
    )

  val ctx = GraphQLContext(ds)

  Http()
    .newServerAt("0.0.0.0", 8088)
    .bind(GraphQLRoute.route(ctx))
}
