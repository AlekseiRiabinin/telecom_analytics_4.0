package com.telecomanalytics.pipelines.kafka

import com.telecomanalytics.core.SparkSessionManager
import com.telecomanalytics.models.SmartMeterRecord
import com.typesafe.config.Config
import org.apache.spark.sql.{SparkSession, Dataset, Encoders}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoder
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory
import java.time.LocalDate

object KafkaToMinio {

  private val logger = LoggerFactory.getLogger(getClass)

  implicit val smartMeterEncoder: Encoder[SmartMeterRecord] =
    Encoders.product[SmartMeterRecord]

  case class Args(
      config: String,
      date: Option[String],
      prod: Boolean
  )

  // -------------------------------
  // MAIN ENTRY POINT
  // -------------------------------
  def main(args: Array[String]): Unit = {
    val parsed = parseArgs(args)
    val config = ConfigFactory.load(parsed.config)
    val processingDate = parsed.date.getOrElse(LocalDate.now().toString)

    logger.info(
      s"Running KafkaToMinio with Dataset, date = $processingDate, " +
        s"prod = ${parsed.prod}"
    )

    val spark = SparkSessionManager.create("KafkaToMinio", config)

    try {
      val ds: Dataset[SmartMeterRecord] =
        if (parsed.prod) readFromKafkaProd(spark, config)
        else createSampleDataset(spark)

      ds.show(10, truncate = false)

      val success: Boolean =
        if (parsed.prod) writeToMinioProd(ds, config, processingDate)
        else writeToLocal(ds, processingDate)

      if (success) logger.info("Pipeline completed successfully")
      else logger.error("Pipeline failed")

    } catch {
      case e: Exception =>
        logger.error(s"Pipeline execution failed: ${e.getMessage}", e)
    } finally {
      spark.stop()
      logger.info("Spark session stopped")
    }
  }

  // -------------------------------
  // DEV: CREATE SAMPLE DATASET
  // -------------------------------
  def createSampleDataset(spark: SparkSession): Dataset[SmartMeterRecord] = {
    import spark.implicits._
    Seq(
      SmartMeterRecord("METER_001", "2024-01-15 10:00:00", 15.75, 220.5, 7.1, 0.95, 50.0),
      SmartMeterRecord("METER_002", "2024-01-15 10:01:00", 22.30, 219.8, 10.2, 0.92, 49.9)
    ).toDS()
  }

  // -------------------------------
  // PROD: READ FROM KAFKA AS DATASET
  // -------------------------------
  def readFromKafkaProd(spark: SparkSession, config: Config): Dataset[SmartMeterRecord] = {
    import spark.implicits._

    val kafkaCfg = config.getConfig("kafka")

    // Raw Kafka JSON as DataFrame (untyped)
    val jsonDF = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaCfg.getString("bootstrap_servers"))
      .option("subscribe", kafkaCfg.getString("topic"))
      .option("startingOffsets", "earliest")
      .load()
      .select(col("value").cast("string").as("json"))

    // Parsed JSON as Dataset (typed)
    val jsonDS: Dataset[SmartMeterRecord] = jsonDF
      .select(from_json($"json", Encoders.product[SmartMeterRecord].schema).as("data"))
      .select("data.*")
      .as[SmartMeterRecord]

    jsonDS
  }

  // -------------------------------
  // DEV: WRITE LOCALLY
  // -------------------------------
  def writeToLocal(ds: Dataset[SmartMeterRecord], date: String): Boolean = {
    val path = s"/tmp/telecom_data/smart_meter/date=$date"
    logger.info(s"Writing to $path")

    try {
      ds.write.mode("overwrite").parquet(path)
      true
    } catch {
      case e: Exception =>
        logger.error(s"Write local failed: ${e.getMessage}", e)
        false
    }
  }

  // -------------------------------
  // PROD: WRITE TO MINIO
  // -------------------------------
  def writeToMinioProd(ds: Dataset[SmartMeterRecord], config: Config, date: String): Boolean = {
    val minio = config.getConfig("minio")
    val outputPath = s"s3a://${minio.getString("raw_bucket")}/smart_meter_data/date=$date"
    logger.info(s"Writing dataset to MinIO: $outputPath")

    try {
      ds.write.mode("append").option("compression", "snappy").parquet(outputPath)
      true
    } catch {
      case e: Exception =>
        logger.error(s"MinIO write failed: ${e.getMessage}", e)
        false
    }
  }

  // -------------------------------
  // ARGUMENT PARSER
  // -------------------------------
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
