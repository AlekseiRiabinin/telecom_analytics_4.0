package com.telecomanalytics.domain


sealed trait VertexType

object VertexType {
  case object Subscriber extends VertexType
  case object Device extends VertexType
  case object Cell extends VertexType
  case object IP extends VertexType
}
