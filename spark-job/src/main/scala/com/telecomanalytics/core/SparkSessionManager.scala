package com.telecomanalytics.core

import org.apache.spark.sql.SparkSession
import com.typesafe.config.Config

object SparkSessionManager {

  def create(appName: String, config: Config): SparkSession = {

    val spark = SparkSession.builder()
      .appName(appName)
      .config("spark.sql.adaptive.enabled", config.getBoolean("spark.sql_adaptive_enabled"))
      .config("spark.driver.memory", config.getString("spark.driver_memory"))
      .config("spark.executor.memory", config.getString("spark.executor_memory"))
      .getOrCreate()

    spark
  }
}
