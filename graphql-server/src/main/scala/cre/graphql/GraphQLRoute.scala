package cre.graphql

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.{HttpEntity, ContentTypes}
import akka.http.scaladsl.marshalling.ToResponseMarshaller

import sangria.execution.Executor
import sangria.parser.QueryParser
import sangria.ast.Document
import sangria.marshalling.circe._

import io.circe._
import io.circe.parser._
import io.circe.syntax._

import scala.util.{Try, Success, Failure}
import scala.concurrent.ExecutionContext


object GraphQLRoute {

  def route(ctx: GraphQLContext)(implicit ec: ExecutionContext) =
    path("graphql") {
      post {
        entity(as[String]) { body =>
          parse(body) match {

            // JSON parsing error
            case Left(jsonErr) =>
              complete(
                StatusCodes.BadRequest,
                Json.obj("error" -> Json.fromString(jsonErr.message)).noSpaces
              )

            case Right(json) =>
              val cursor = json.hcursor

              val query =
                cursor.get[String]("query").getOrElse("")

              val variables =
                cursor.get[Json]("variables").getOrElse(Json.obj())

              QueryParser.parse(query) match {

                // GraphQL parsing error
                case Failure(err) =>
                  complete(
                    StatusCodes.BadRequest,
                    Json.obj("error" -> Json.fromString(err.getMessage)).noSpaces
                  )

                case Success(ast: Document) =>
                  complete(
                    Executor
                      .execute(
                        schema = SchemaDef.schema,
                        queryAst = ast,
                        userContext = ctx,
                        variables = variables
                      )
                      .map { json =>
                        akka.http.scaladsl.model.HttpEntity(
                          akka.http.scaladsl.model.ContentTypes.`application/json`,
                          json.noSpaces
                        )
                      }
                  )
              }
          }
        }
      }
    }
}
