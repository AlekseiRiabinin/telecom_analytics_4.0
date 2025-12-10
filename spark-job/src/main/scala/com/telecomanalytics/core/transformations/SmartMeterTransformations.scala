package com.telecomanalytics.core.transformations

import com.telecomanalytics.models.SmartMeterRecord
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object SmartMeterTransformations {

  def filled(raw: Dataset[SmartMeterRecord]): Dataset[SmartMeterRecord] = {
    import raw.sparkSession.implicits._

    raw.toDF()
      .withColumn("energy_consumption", col("energy_consumption").cast("double"))
      .withColumn("current_reading",    col("current_reading").cast("double"))
      .withColumn("power_factor",       col("power_factor").cast("double"))
      .withColumn("frequency",          col("frequency").cast("double"))
      .withColumn("voltage",            col("voltage").cast("double"))
      .withColumn("meter_id",           col("meter_id").cast("string"))
      .withColumn("timestamp",          col("timestamp").cast("string"))
      .na.fill(Map(
        "energy_consumption" -> 15.0,
        "current_reading"    -> 10.0,
        "power_factor"       -> 0.95,
        "frequency"          -> 50.0,
        "voltage"            -> 220.0
      ))
      .as[SmartMeterRecord]
  }

}
