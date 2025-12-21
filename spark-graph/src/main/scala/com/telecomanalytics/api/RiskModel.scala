package com.telecomanalytics.api


object RiskModel extends Model {

  /**
   * Example risk definition:
   * risk = avg(edge.weight) * max(vertex.value)
   */
  override def formula(): Formula =
    Mul(
      AvgEdges(Const(1.0)),
      MaxVertices(Const(1.0))
    )
}
