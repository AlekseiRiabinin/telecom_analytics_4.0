package com.telecomanalytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Test Pipeline - works with local files for development
 */
object TestPipeline {
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder()
      .appName("TelecomTest")
      .master("local[*]")
      .getOrCreate()

    try {
      println("Starting test pipeline...")
      
      // Create simple test data
      import spark.implicits._
      val testData = Seq(
        ("BTS_001", "MTS", "Moscow", 45.5, 1200.5, 1500),
        ("BTS_002", "Megafon", "Moscow", 52.3, 1100.2, 1300),
        ("BTS_003", "Beeline", "SPB", 48.7, 980.7, 1100)
      ).toDF("equipment_id", "operator", "region", "cpu_usage", "throughput", "connections")
      
      println("Test data:")
      testData.show()
      
      // Simple aggregation
      val result = testData
        .groupBy("operator", "region")
        .agg(
          count("*").as("site_count"),
          avg("cpu_usage").as("avg_cpu"),
          avg("throughput").as("avg_throughput")
        )
      
      println("Aggregated result:")
      result.show()
      
      println("Test pipeline completed!")
      
    } catch {
      case e: Exception =>
        println(s"Test failed: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
