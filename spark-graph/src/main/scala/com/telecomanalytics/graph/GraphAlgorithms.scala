package com.telecomanalytics.graph

import com.telecomanalytics.domain._


object GraphAlgorithms {

  def simpleScore(graph: GraphSchema): Map[String, Double] =
    graph.vertices.map(v => v.id -> v.value).toMap
}
