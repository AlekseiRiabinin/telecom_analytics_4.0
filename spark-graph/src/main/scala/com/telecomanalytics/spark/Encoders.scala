package com.telecomanalytics.spark

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders._

import com.telecomanalytics.domain._


object Encoders {

  implicit val vertexEncoder: Encoder[VertexAttr] = product[VertexAttr]
  implicit val edgeEncoder: Encoder[EdgeAttr] = product[EdgeAttr]
}
