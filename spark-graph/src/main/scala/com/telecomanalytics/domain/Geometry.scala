package com.telecomanalytics.domain


sealed trait Geometry {
  def srid: Int
}

object Geometry {

  final case class Point(
    lon: Double,
    lat: Double,
    srid: Int = 4326
  ) extends Geometry

  final case class LineString(
    points: Seq[Point],
    srid: Int = 4326
  ) extends Geometry

  final case class Polygon(
    rings: Seq[Seq[Point]],
    srid: Int = 4326
  ) extends Geometry
}
