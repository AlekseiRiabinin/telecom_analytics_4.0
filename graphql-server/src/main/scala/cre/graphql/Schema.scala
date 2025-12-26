package cre.graphql

import sangria.schema._
import cre.resolvers._


object SchemaDef {

  val FeatureType: ObjectType[GraphQLContext, Map[String, Any]] =
    ObjectType(
      "Feature",
      fields[GraphQLContext, Map[String, Any]](
        Field(
          "id",
          StringType,
          resolve = _.value("id").toString
        ),
        Field(
          "risk",
          FloatType,
          resolve = _.value("risk").asInstanceOf[Double]
        )
      )
    )

  val QueryType = ObjectType(
    "Query",
    fields[GraphQLContext, Unit](
      Field(
        "featuresWithin",
        ListType(FeatureType),
        arguments = Argument("lon", FloatType) ::
                    Argument("lat", FloatType) ::
                    Argument("radiusMeters", FloatType) :: Nil,
        resolve = SpatialResolver.featuresWithin
      )
    )
  )

  val schema = Schema(QueryType)
}
