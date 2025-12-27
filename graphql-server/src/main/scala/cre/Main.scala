package cre

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import cre.db.Database
import cre.graphql._
import cre.config.AppConfig

import scala.concurrent.ExecutionContextExecutor


object Main extends App {

  implicit val system: ActorSystem = ActorSystem("graphql-cre")
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  // Initialize DataSource from AppConfig
  val ds = Database.dataSource(
    AppConfig.dbConfig.host,
    AppConfig.dbConfig.port,
    AppConfig.dbConfig.name,
    AppConfig.dbConfig.user,
    AppConfig.dbConfig.password
  )

  val ctx = GraphQLContext(ds)

  // Get bind port from AppConfig
  val port = AppConfig.graphqlPort.getOrElse(8088)

  Http()
    .newServerAt("0.0.0.0", port)
    .bind(GraphQLRoute.route(ctx))
    .foreach { binding =>
      system.log.info(
        s"GraphQL server running at http://0.0.0.0:$port${AppConfig.graphqlPort}"
      )
    }
}
