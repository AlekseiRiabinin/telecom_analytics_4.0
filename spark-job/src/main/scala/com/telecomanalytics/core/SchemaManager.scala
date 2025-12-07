package com.telecomanalytics.core

import org.apache.spark.sql.types._

object SchemaManager {

  def smartMeterSchema: StructType = StructType(Seq(
    StructField("meter_id", StringType),
    StructField("timestamp", StringType),
    StructField("energy_consumption", DoubleType),
    StructField("voltage", DoubleType),
    StructField("current_reading", DoubleType),
    StructField("power_factor", DoubleType),
    StructField("frequency", DoubleType)
  ))
}
