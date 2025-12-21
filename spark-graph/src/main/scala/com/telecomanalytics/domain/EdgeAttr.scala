package com.telecomanalytics.domain


case class EdgeAttr(
  src: String,
  dst: String,
  weight: Double,
  eType: EdgeType = EdgeType.Call,
  properties: Map[String, Any] = Map.empty,
  timestamp: Long = System.currentTimeMillis()
)
