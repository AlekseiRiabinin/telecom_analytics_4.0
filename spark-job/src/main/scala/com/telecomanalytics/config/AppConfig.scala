package com.telecomanalytics.config

import com.typesafe.config.{Config, ConfigFactory}

object AppConfig {
  
  private val config: Config = ConfigFactory.load()
  
  // Spark configuration
  object Spark {
    val appName: String = config.getString("spark.app.name")
    val master: String = config.getString("spark.master")
    val executorMemory: String = config.getString("spark.executor.memory")
    val driverMemory: String = config.getString("spark.driver.memory")
  }
  
  // Data source configuration
  object DataSource {
    val hdfsInputPath: String = config.getString("data.source.hdfs.input")
    val hdfsOutputPath: String = config.getString("data.source.hdfs.output")
  }
  
  // ClickHouse configuration
  object ClickHouse {
    val url: String = config.getString("clickhouse.url")
    val user: String = config.getString("clickhouse.user")
    val password: String = config.getString("clickhouse.password")
    val database: String = config.getString("clickhouse.database")
  }
  
  // Application settings
  object App {
    val processingWindowHours: Int = config.getInt("app.processing.window.hours")
    val anomalyThreshold: Double = config.getDouble("app.anomaly.threshold")
    val enableMetricsExport: Boolean = config.getBoolean("app.metrics.export.enabled")
  }
}
