package com.telecomanalytics.pipelines.minio

import com.telecomanalytics.core.SparkSessionManager
import com.telecomanalytics.core.transformations.SmartMeterTransformations
import com.telecomanalytics.models.SmartMeterRecord
import org.apache.spark.sql.{SparkSession, Dataset, Encoder, Encoders, Row}
import org.apache.spark.sql.functions._
import com.typesafe.config.{ConfigFactory, Config}
import org.slf4j.LoggerFactory


import java.net.{URL, HttpURLConnection}
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}


object MinioToClickHouse {

  private val logger = LoggerFactory.getLogger(getClass)

  implicit val smartMeterEncoder: Encoder[SmartMeterRecord] = 
    Encoders.product[SmartMeterRecord]

  case class ClickHouseRecord(
    meter_id: String,
    timestamp: Timestamp,
    energy_consumption: Double,
    voltage: Double,
    current_reading: Double,
    power_factor: Double,
    frequency: Double,
    date: Date,
    year: Int,
    month: Int,
    day: Int,
    hour: Int,
    minute: Int,
    consumption_category: String,
    is_anomaly: Int,
    partition_date: Date,
    processed_at: Timestamp
  )
  implicit val chEncoder: Encoder[ClickHouseRecord] = Encoders.product[ClickHouseRecord]

  case class Args(config: String, date: Option[String], prod: Boolean)

  def run(configPath: String, processingDate: String): Boolean = {
    val config = ConfigFactory
      .parseResourcesAnySyntax(configPath)
      .withFallback(ConfigFactory.load())
    val spark = SparkSessionManager.create("MinioToClickHouse_ETL", config)

    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728")

    try {
      logger.info(s"Starting MinIO → ClickHouse ETL for date: $processingDate")

      val rawDS = readFromMinio(spark, config, processingDate)
      if (rawDS.isEmpty) {
        logger.warn("No data found to process (empty or missing)")
        return true
      }
      val raw = rawDS.get

      val rawCount = raw.count()
      logger.info(s"Raw data count: $rawCount")
      if (rawCount == 0) return true

      val transformed = transformForClickHouse(raw)
      val transformedCount = transformed.count()
      logger.info(s"Transformed data count: $transformedCount")
      if (transformedCount == 0) return true

      val aggregations = createClickHouseAggregations(transformed, spark)

      val writeSuccess = writeToClickHouse(config, aggregations)
      if (writeSuccess) {
        logger.info("MinIO to ClickHouse ETL completed successfully")
        aggregations.foreach { case (name, ds) =>
          logger.info(s"  $name: ${ds.count()} records")
        }
      } else {
        logger.error("ClickHouse ETL failed")
      }

      writeSuccess
    } catch {
      case ex: Throwable =>
        logger.error("ETL pipeline execution failed", ex)
        false
    } finally {
      spark.stop()
      logger.info("Spark session stopped")
    }
  }

  def readFromMinio(
    spark: SparkSession, config: Config, processingDate: String)
      : Option[Dataset[SmartMeterRecord]] = {

    import spark.implicits._

    Try {
      val minio = config.getConfig("minio")
      val rawBucket = minio.getString("raw_bucket")
      val inputPath = s"s3a://$rawBucket/smart_meter_data/date=$processingDate"

      logger.info(s"Reading from MinIO: $inputPath")

      val df = spark.read.parquet(inputPath)
      logger.info("Schema read from Parquet:")
      df.printSchema()
      logger.info("Sample rows:")
      df.show(5, truncate = false)

      val maybeDS = Try(df.as[SmartMeterRecord]).toOption
      maybeDS.orElse {
        val dfSel = df
          .withColumn("energy_consumption",
            coalesce(col("energy_consumption").cast("double"), lit(0.0))
          )
          .withColumn("voltage",
            coalesce(col("voltage").cast("double"), lit(0.0))
          )
          .withColumn("current_reading",
            coalesce(col("current_reading").cast("double"), lit(0.0))
          )
          .withColumn("power_factor",
            coalesce(col("power_factor").cast("double"), lit(0.0))
          )
          .withColumn("frequency",
            coalesce(col("frequency").cast("double"), lit(0.0))
          )
          .withColumn("meter_id",
            col("meter_id").cast("string")
          )
          .withColumn("timestamp",
            col("timestamp").cast("string")
          )

        Try(dfSel.select(
          $"meter_id",
          $"timestamp",
          $"energy_consumption",
          $"voltage",
          $"current_reading",
          $"power_factor",
          $"frequency"
        ).as[SmartMeterRecord]).toOption
      }
    }.toOption.flatten
  }

  def transformForClickHouse(raw: Dataset[SmartMeterRecord]): Dataset[ClickHouseRecord] = {

    logger.info("Applying ClickHouse-optimized transformations")

    val filled = SmartMeterTransformations.filled(raw)
    val validated = DataQualityChecker.validateMeterData(filled)

    val df = validated.toDF()
      .withColumn("timestamp_ts", to_timestamp(col("timestamp")))
      .withColumn("date", to_date(col("timestamp_ts")))
      .withColumn("year", year(col("timestamp_ts")))
      .withColumn("month", month(col("timestamp_ts")))
      .withColumn("day", dayofmonth(col("timestamp_ts")))
      .withColumn("hour", hour(col("timestamp_ts")))
      .withColumn("minute", minute(col("timestamp_ts")))
      .withColumn("consumption_category",
        when(col("energy_consumption") < 10, "LOW")
          .when(col("energy_consumption") < 25, "MEDIUM")
          .otherwise("HIGH")
      )
      .withColumn("is_anomaly",
        when(col("energy_consumption") > 50 ||
            col("voltage") < 200 ||
            col("voltage") > 250, lit(1)).otherwise(lit(0))
      )
      .withColumn("partition_date", to_date(col("timestamp_ts")))
      .withColumn("processed_at", current_timestamp())
      .withColumnRenamed("timestamp_ts", "timestamp")

    val chDS = df.select(
      col("meter_id").cast("string"),
      col("timestamp").cast("timestamp"),
      col("energy_consumption").cast("double"),
      col("voltage").cast("double"),
      col("current_reading").cast("double"),
      col("power_factor").cast("double"),
      col("frequency").cast("double"),
      col("date").cast("date"),
      col("year").cast("int"),
      col("month").cast("int"),
      col("day").cast("int"),
      col("hour").cast("int"),
      col("minute").cast("int"),
      col("consumption_category").cast("string"),
      col("is_anomaly").cast("int"),
      col("partition_date").cast("date"),
      col("processed_at").cast("timestamp")
    ).as[ClickHouseRecord]

    val cnt = chDS.count()
    logger.info(s"Transformations completed - $cnt records")

    chDS
  }


  def createClickHouseAggregations(ds: Dataset[ClickHouseRecord], spark: SparkSession)
    : Map[String, Dataset[_]] = {

    import spark.implicits._

    logger.info("Creating ClickHouse aggregations")

    val rawData = ds

    val hourly = ds.groupBy($"meter_id", $"date", $"hour", $"partition_date")
      .agg(
        sum($"energy_consumption").alias("total_energy_hourly"),
        avg($"energy_consumption").alias("avg_energy_hourly"),
        avg($"voltage").alias("avg_voltage_hourly"),
        avg($"current_reading").alias("avg_current_hourly"),
        max($"energy_consumption").alias("max_consumption_hourly"),
        min($"energy_consumption").alias("min_consumption_hourly"),
        count(lit(1)).alias("record_count_hourly"),
        sum($"is_anomaly").alias("anomaly_count_hourly")
      )
      .withColumn("aggregation_type", lit("HOURLY"))

    val daily = ds.groupBy($"meter_id", $"date", $"partition_date")
      .agg(
        sum($"energy_consumption").alias("total_energy_daily"),
        avg($"energy_consumption").alias("avg_energy_daily"),
        avg($"voltage").alias("avg_voltage_daily"),
        avg($"current_reading").alias("avg_current_daily"),
        max($"energy_consumption").alias("max_consumption_daily"),
        min($"energy_consumption").alias("min_consumption_daily"),
        stddev($"energy_consumption").alias("std_energy_daily"),
        count(lit(1)).alias("record_count_daily"),
        sum($"is_anomaly").alias("anomaly_count_daily")
      )
      .withColumn("aggregation_type", lit("DAILY"))

    val meterAgg = ds.groupBy($"meter_id", $"partition_date")
      .agg(
        sum($"energy_consumption").alias("total_energy_meter"),
        avg($"energy_consumption").alias("avg_energy_meter"),
        max($"energy_consumption").alias("peak_consumption"),
        count(lit(1)).alias("total_readings"),
        approx_count_distinct($"date").alias("active_days"),
        sum($"is_anomaly").alias("total_anomalies")
      )
      .withColumn("aggregation_type", lit("METER_SUMMARY"))

    val aggregations: Map[String, Dataset[_]] = Map(
      "raw_data" -> rawData,
      "hourly_aggregates" -> hourly,
      "daily_aggregates" -> daily,
      "meter_aggregates" -> meterAgg
    )

    aggregations.foreach { case (name, df) =>
      logger.info(s"   $name: ${df.count()} records")
    }

    logger.info("ClickHouse aggregations created successfully")
    aggregations
  }

  def writeToClickHouse(config: com.typesafe.config.Config, dfs: Map[String, Dataset[_]]): Boolean = {
    import scala.concurrent.ExecutionContext.Implicits.global

    try {
      val ch = config.getConfig("clickhouse")
      val host = ch.getString("host")
      val port = ch.getString("http_port")
      val user = ch.getString("user")
      val password = ch.getString("password")
      val baseUrl = s"http://$host:$port/"

      // quick connection test
      val testResp = postToClickHouse(baseUrl, user, password, "SELECT 1", "", 10.seconds)
      if (!testResp._1) {
        logger.error(s"ClickHouse connection test failed: ${testResp._2}")
        return false
      }

      var totalInserted = 0L

      // For each dataset, partition, create batches from toJSON and POST
      dfs.foreach { case (dfName, anyDs) =>
        val ds = anyDs.asInstanceOf[Dataset[Row]] // for toJSON
        val recordCount = ds.count()
        if (recordCount == 0) {
          logger.info(s"No data to insert for $dfName")
        } else {
          val table = getTableName(dfName)
          logger.info(s"Inserting $recordCount records to $table (df: $dfName)")

          val optimalPartitions = Math.min(32, Math.max(8, (recordCount / 50000).toInt))
          val partDs = ds.repartition(optimalPartitions)
          val jsonRdd = partDs.toJSON.rdd
          val batches = jsonRdd.glom().collect() // array of partitions -> each partition: Array[String]

          logger.info(s"Processing ${batches.length} batches for $dfName")

          // Use a fixed thread pool to parallelize HTTP inserts
          val maxWorkers = Math.min(16, Math.max(1, batches.length))
          implicit val ec = ExecutionContext.fromExecutor(java.util.concurrent.Executors.newFixedThreadPool(maxWorkers))

          val futures = batches.zipWithIndex.filter(_._1.nonEmpty).map { case (batchArr, idx) =>
            Future {
              val payload = batchArr.mkString("\n")
              val query = s"INSERT INTO telecom_analytics.$table FORMAT JSONEachRow"
              val (ok, resp) = postToClickHouse(baseUrl, user, password, query, payload, 120.seconds)
              if (ok) {
                val inserted = batchArr.length
                logger.debug(s"Batch $idx for $dfName inserted $inserted rows")
                inserted
              } else {
                logger.error(s"Batch $idx for $dfName failed: $resp")
                0
              }
            }(ec)
          }

          // Await all and count successes
          val results = Await.result(Future.sequence(futures.toList)(implicitly, ec), Duration.Inf)
          val insertedInDf = results.sum
          totalInserted += insertedInDf
          val successfulBatches = results.count(_ > 0)
          logger.info(s"$dfName: $successfulBatches/${batches.length} batches, $insertedInDf/$recordCount records")
          if (successfulBatches == 0) {
            logger.error(s"All batches failed for $dfName")
            return false
          }
        }
      }

      logger.info(s"Total inserted across all tables: $totalInserted records")
      totalInserted > 0
    } catch {
      case ex: Throwable =>
        logger.error("Write to ClickHouse failed", ex)
        false
    }
  }

  // ----- helper: perform HTTP POST to ClickHouse -----
  private def postToClickHouse(baseUrl: String, user: String, password: String, query: String, payload: String, timeout: FiniteDuration): (Boolean, String) = {
    Try {
      // Build URL with query param
      val urlStr = s"$baseUrl?query=${java.net.URLEncoder.encode(query, "UTF-8")}"
      val url = new URL(urlStr)
      val conn = url.openConnection().asInstanceOf[HttpURLConnection]
      try {
        conn.setConnectTimeout(timeout.toMillis.toInt)
        conn.setReadTimeout(timeout.toMillis.toInt)
        conn.setDoOutput(true)
        conn.setRequestMethod("POST")
        conn.setRequestProperty("Content-Type", "text/plain; charset=utf-8")
        // Basic auth if provided
        if (user != null && user.nonEmpty) {
          val auth = java.util.Base64.getEncoder.encodeToString(s"$user:$password".getBytes(StandardCharsets.UTF_8))
          conn.setRequestProperty("Authorization", s"Basic $auth")
        }

        if (payload != null && payload.nonEmpty) {
          val bytes = payload.getBytes(StandardCharsets.UTF_8)
          conn.setFixedLengthStreamingMode(bytes.length)
          val out = conn.getOutputStream
          try out.write(bytes)
          finally out.close()
        } else {
          conn.setFixedLengthStreamingMode(0)
        }

        val code = conn.getResponseCode
        val respStream = if (code >= 200 && code < 300) conn.getInputStream else conn.getErrorStream
        val resp = scala.io.Source.fromInputStream(respStream, "UTF-8").mkString
        (code >= 200 && code < 300, resp)
      } finally {
        conn.disconnect()
      }
    } match {
      case Success(v) => v
      case Failure(ex) =>
        (false, ex.getMessage)
    }
  }

  // ----- map df name to ClickHouse table -----
  private def getTableName(dfName: String): String = {
    val tableMapping = Map(
      "raw_data" -> "smart_meter_raw",
      "hourly_aggregates" -> "meter_aggregates",
      "daily_aggregates" -> "meter_aggregates",
      "meter_aggregates" -> "meter_aggregates"
    )
    tableMapping.getOrElse(dfName, dfName)
  }

  // Placeholder for DataQualityChecker: you should implement your own quality checks.
  // For now, it returns the input dataset unchanged.
  object DataQualityChecker {
    def validateMeterData(ds: Dataset[SmartMeterRecord]): Dataset[SmartMeterRecord] = {
      // Implement your checks (range checks, null filters, outlier detection...)
      // For now we just return ds unchanged.
      ds
    }
  }
}
