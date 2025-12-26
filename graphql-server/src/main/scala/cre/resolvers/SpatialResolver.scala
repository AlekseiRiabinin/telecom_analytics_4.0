package cre.resolvers

import sangria.schema.Context
import cre.graphql.GraphQLContext

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import java.sql.Connection


// GraphQL → SQL view/function → PostGIS → Graph analytics
object SpatialResolver {

  def featuresWithin(ctx: Context[GraphQLContext, Unit]): Future[List[Map[String, Any]]] =
    Future {
      val ds = ctx.ctx.dataSource
      val conn: Connection = ds.getConnection

      try {
        val stmt =
          conn.prepareStatement(
            """
              SELECT id, risk
              FROM api.features_within_radius(
                ?, ?, ?
              )
            """
          )

        stmt.setDouble(1, ctx.arg[Double]("lon"))
        stmt.setDouble(2, ctx.arg[Double]("lat"))
        stmt.setDouble(3, ctx.arg[Double]("radiusMeters"))

        val rs = stmt.executeQuery()
        Iterator
          .continually(rs)
          .takeWhile(_.next())
          .map { _ =>
            Map(
              "id" -> rs.getString("id"),
              "risk" -> rs.getDouble("risk")
            )
          }
          .toList
      } finally conn.close()
    }
}
