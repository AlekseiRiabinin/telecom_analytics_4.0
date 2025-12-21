package com.telecomanalytics.domain


case class VertexAttr(
  id: String,
  value: Double,
  vType: VertexType = VertexType.Subscriber,
  properties: Map[String, Any] = Map.empty,
  createdAt: Long = System.currentTimeMillis()
)
