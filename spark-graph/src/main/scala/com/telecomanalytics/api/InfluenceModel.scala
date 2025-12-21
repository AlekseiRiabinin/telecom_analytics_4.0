package com.telecomanalytics.api


/**
 * Influence model:
 * measures how influential a node is based on
 * outgoing edge weights and connected vertices
 */
object InfluenceModel extends Model {

  override def formula(): Formula =
    Normalize(SumEdges(EdgeField("weight")))
}
