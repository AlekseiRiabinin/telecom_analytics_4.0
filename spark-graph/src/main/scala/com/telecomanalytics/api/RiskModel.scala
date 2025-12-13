package com.telecomanalytics.api

import org.apache.spark.sql.{Dataset, SparkSession}

case class RiskResult(nodeId: String, risk: Double)


class RiskModel(spark: SparkSession) extends Model[RiskResult] {

  import spark.implicits._

  override def evaluate(): Dataset[RiskResult] = {
    Seq(
      RiskResult("A", 0.3),
      RiskResult("B", 0.7)
    ).toDS()
  }
}
