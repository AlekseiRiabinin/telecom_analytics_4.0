package com.telecomanalytics.postgis


import com.telecomanalytics.domain.Geometry

/**
 * Maps domain Geometry into PostGIS SQL expressions.
 *
 * NOTE:
 * - Domain stays backend-agnostic
 * - SQL strings are generated ONLY here
 */
object PostgisGeometryMapper {

  val DefaultSRID: Int = 4326

  /** Converts Geometry to WKT (no SRID, pure text) */
  def toWKT(geometry: Geometry): String =
    geometry match {

      case Geometry.Point(lon, lat, _) =>
        s"POINT($lon $lat)"

      case Geometry.LineString(points, _) =>
        val coords =
          points.map(p => s"${p.lon} ${p.lat}").mkString(", ")
        s"LINESTRING($coords)"

      case Geometry.Polygon(rings, _) =>
        val ringWkts = rings.map { ring =>
          ring.map(p => s"${p.lon} ${p.lat}").mkString("(", ", ", ")")
        }.mkString(", ")
        s"POLYGON($ringWkts)"
    }

  /** Geometry SQL expression with SRID */
  def toGeometrySQL(
    geometry: Geometry,
    srid: Int = DefaultSRID
  ): String =
    s"ST_SetSRID(ST_GeomFromText('${toWKT(geometry)}'), $srid)"

  /** Geography SQL expression */
  def toGeographySQL(
    geometry: Geometry,
    srid: Int = DefaultSRID
  ): String =
    s"${toGeometrySQL(geometry, srid)}::geography"
}
