package com.telecomanalytics.domain


sealed trait EdgeType

object EdgeType {
  case object Call extends EdgeType
  case object SMS extends EdgeType
  case object DataSession extends EdgeType
  case object ConnectedTo extends EdgeType
}
