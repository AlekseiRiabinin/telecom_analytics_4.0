package com.telecomanalytics.api

import org.apache.spark.sql.{Dataset, SparkSession}


case class InfluenceResult(nodeId: String, score: Double)

class InfluenceModel(spark: SparkSession) extends Model[InfluenceResult] {

  import spark.implicits._

  override def evaluate(): Dataset[InfluenceResult] = {
    Seq(
      InfluenceResult("A", 10.0),
      InfluenceResult("B", 5.0)
    ).toDS()
  }
}
