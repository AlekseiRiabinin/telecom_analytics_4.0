package com.telecomanalytics.graph

import com.telecomanalytics.domain._


object GraphLoader {

  def loadSampleGraph(): GraphSchema =
    GraphSchema(
      vertices = Seq(
        VertexAttr("A", 1.0),
        VertexAttr("B", 2.0)
      ),
      edges = Seq(
        EdgeAttr("A", "B", 1.0)
      )
    )
}
