package com.telecomanalytics.graph

import com.telecomanalytics.domain._


object GraphOps {

  def degree(vertexId: String, graph: GraphSchema): Int =
    graph.edges.count(e => e.src == vertexId || e.dst == vertexId)
}
