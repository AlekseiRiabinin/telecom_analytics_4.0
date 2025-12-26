package cre.graphql

import javax.sql.DataSource


final case class GraphQLContext(
  dataSource: DataSource
)
