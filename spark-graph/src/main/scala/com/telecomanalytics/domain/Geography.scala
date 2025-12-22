package com.telecomanalytics.domain


sealed trait Geography {
  def srid: Int
}

object Geography {

  final case class GeoPoint(
    lon: Double,
    lat: Double,
    srid: Int = 4326
  ) extends Geography

}
