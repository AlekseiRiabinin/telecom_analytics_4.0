package com.telecomanalytics.domain

case class GraphSchema(
  vertices: Seq[VertexAttr],
  edges: Seq[EdgeAttr]
)
