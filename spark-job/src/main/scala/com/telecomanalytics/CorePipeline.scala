package com.telecomanalytics

import org.apache.spark.sql.{SparkSession, SaveMode, Dataset}
import com.telecomanalytics.domain.{NetworkTelemetry, TelecomSummary}

object CorePipeline {
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder()
      .appName("TelecomDatasetPipeline")
      .getOrCreate()

    import spark.implicits._

    try {
      println("Starting telecom pipeline with Dataset API...")
      
      // 1. Read data as Dataset[NetworkTelemetry]
      val telemetryDS: Dataset[NetworkTelemetry] = spark.read
        .option("header", "true")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .csv("hdfs://hdfs-namenode:9000/data/telecom/network_equipment/*.csv")
        .as[NetworkTelemetry]
      
      println(s"Loaded ${telemetryDS.count()} records")
      telemetryDS.show(5)

      // 2. Transformation with explicit type conversions
      val summaryDS: Dataset[TelecomSummary] = telemetryDS
        .filter(_.operator != null)
        .groupByKey(telemetry => (telemetry.operator, telemetry.region))
        .mapGroups { (key, iterator) =>
          val operator = key._1
          val region = key._2
          val records = iterator.toList
          val equipmentCount = records.size.toLong
          val avgCpuUsage = 
            if (equipmentCount > 0) records.map(_.cpuUsage).sum / equipmentCount 
            else 0.0
          val avgThroughput = 
            if (equipmentCount > 0) records.map(_.networkThroughput).sum / equipmentCount 
            else 0.0
          val totalConnections = records.map(_.activeConnections.toLong).sum
          
          TelecomSummary(
            operator,
            region,
            equipmentCount,
            avgCpuUsage,
            avgThroughput,
            totalConnections
          )
        }
      
      println("Transformed results:")
      summaryDS.show(10)
      
      // 3. Write to ClickHouse
      summaryDS
        .toDF()
        .write
        .format("jdbc")
        .option("url", "jdbc:clickhouse://clickhouse:8123/default")
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("dbtable", "telecom_summary")
        .option("user", "default")
        .option("password", "")
        .mode(SaveMode.Overwrite)
        .save()
      
      println("Pipeline completed successfully!")
      
    } catch {
      case e: Exception =>
        println(s"Error: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
