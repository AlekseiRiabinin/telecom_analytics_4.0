package com.telecomanalytics.pipelines.kafka

import com.telecomanalytics.core.{SparkSessionManager}
import com.telecomanalytics.models.SmartMeterRecord
import com.telecomanalytics.utils.Logging

import org.apache.spark.sql.{SparkSession, Dataset, Encoders}
import org.apache.spark.sql.functions._
import com.typesafe.config.ConfigFactory

import java.time.LocalDate

object KafkaToMinio extends Logging {

  implicit val smartMeterEncoder = Encoders.product[SmartMeterRecord]

  case class Args(
      config: String,
      date: Option[String],
      prod: Boolean
  )

  def main(args: Array[String]): Unit = {

    val parsed = parseArgs(args)
    val config = ConfigFactory.load(parsed.config)
    val processingDate = parsed.date.getOrElse(LocalDate.now().toString)

    logInfo(s"Running KafkaToMinio with Dataset, date = $processingDate")

    val spark = SparkSessionManager.create("KafkaToMinio", config)

    try {
      val ds: Dataset[SmartMeterRecord] =
        if (parsed.prod) readFromKafkaProd(spark, config)
        else createSampleDataset(spark)

      ds.show(10, truncate = false)

      val success =
        if (parsed.prod)
          writeToMinioProd(ds, config, processingDate)
        else
          writeToLocal(ds, processingDate)

      if (success) logInfo("Pipeline completed successfully")
      else logError("Pipeline failed")

    } finally {
      spark.stop()
    }
  }

  // ------------------------------------------
  // DEV SAMPLE DATASET
  // ------------------------------------------
  def createSampleDataset(spark: SparkSession): Dataset[SmartMeterRecord] = {
    import spark.implicits._

    Seq(
      SmartMeterRecord("METER_001", "2024-01-15 10:00:00", 15.75, 220.5, 7.1, 0.95, 50.0),
      SmartMeterRecord("METER_002", "2024-01-15 10:01:00", 22.30, 219.8, 10.2, 0.92, 49.9)
    ).toDS()
  }

  // ------------------------------------------
  // PROD: READ FROM KAFKA AS DATASET
  // ------------------------------------------
  def readFromKafkaProd(
      spark: SparkSession,
      config: com.typesafe.config.Config
  ): Dataset[SmartMeterRecord] = {

    import spark.implicits._

    val kafkaCfg = config.getConfig("kafka")

    val jsonDF = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaCfg.getString("bootstrap_servers"))
      .option("subscribe", kafkaCfg.getString("topic"))
      .option("startingOffsets", "earliest")
      .load()
      .select(col("value").cast("string").as("json"))

    // Parse JSON directly into Dataset[T]
    jsonDF
      .select(from_json($"json", Encoders.product[SmartMeterRecord].schema).as("data"))
      .select("data.*")
      .as[SmartMeterRecord]
  }

  // ------------------------------------------
  // DEV: WRITE LOCALLY
  // ------------------------------------------
  def writeToLocal(ds: Dataset[SmartMeterRecord], date: String): Boolean = {
    val path = s"/tmp/telecom_data/smart_meter/date=$date"
    logInfo(s"Writing to $path")

    try {
      ds.write.mode("overwrite").parquet(path)
      true
    } catch {
      case e: Exception =>
        logError("Write local failed: " + e.getMessage)
        false
    }
  }

  // ------------------------------------------
  // PROD: WRITE TO MINIO
  // ------------------------------------------
  def writeToMinioProd(
      ds: Dataset[SmartMeterRecord],
      config: com.typesafe.config.Config,
      date: String
  ): Boolean = {

    val minio = config.getConfig("minio")
    val outputPath = s"s3a://${minio.getString("raw_bucket")}/smart_meter_data/date=$date"

    logInfo(s"Writing dataset to MinIO: $outputPath")

    try {
      ds.write
        .mode("append")
        .option("compression", "snappy")
        .parquet(outputPath)
      true
    } catch {
      case e: Exception =>
        logError("MinIO write failed: " + e.getMessage)
        false
    }
  }

  // ------------------------------------------
  // ARGUMENT PARSER
  // ------------------------------------------
  def parseArgs(args: Array[String]): Args = {
    var config = "etl_dev.conf"
    var date: Option[String] = None
    var prod = false

    args.sliding(2, 1).foreach {
      case Array("--config", c) => config = c
      case Array("--date", d)   => date = Some(d)
      case Array("--prod", _)   => prod = true
      case _                    =>
    }

    Args(config, date, prod)
  }
}
