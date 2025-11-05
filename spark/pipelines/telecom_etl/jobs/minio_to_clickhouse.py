"""
MinIO to ClickHouse Spark Job - Optimized for ClickHouse Hook.
"""

import sys
import os
import logging
import argparse
import configparser
from typing import TypedDict, cast
from argparse import Namespace
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from spark.pipelines.shared.utils.spark_utils import (
    SparkSessionManager,
    DataQualityChecker
)    
from pyspark.sql.functions import *
from pyspark.sql.types import *


class Arguments(TypedDict):
    config: str
    date: str | None
    prod: bool


class MinioToClickHouse:
    """Read from MinIO, transform, write to ClickHouse."""
    
    def __init__(
        self: 'MinioToClickHouse',
        config_path: str,
        processing_date: datetime = None
    ) -> None:
        self.config_path = config_path
        self.processing_date = processing_date
        self.spark = None
        self.logger = self.setup_logging()

    def setup_logging(self: 'MinioToClickHouse') -> logging:
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger('MinioToClickHouse')
    
    def initialize_spark(self: 'MinioToClickHouse') -> SparkSession:
        """Initialize Spark session with ClickHouse optimization."""

        self.logger.info("Initializing Spark session for ClickHouse ETL")
        self.spark = SparkSessionManager.create_spark_session(
            "MinioToClickHouse_ETL", 
            self.config_path
        )
        
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
        
        return self.spark

    def read_from_minio(self: 'MinioToClickHouse') -> DataFrame:
        """Read data from MinIO with error handling."""

        try:
            config = configparser.ConfigParser()
            config.read(self.config_path)
            minio_config = config['minio']
            
            input_path = (
                f"s3a://{minio_config['raw_bucket']}"
                f"/smart_meter_data/date={self.processing_date}"
            )

            self.logger.info(f"Reading from MinIO: {input_path}")

            df = self.spark.read.parquet(input_path)
            
            record_count = df.count()
            self.logger.info(f"Successfully read {record_count} records from MinIO")
            
            self.logger.info("Data schema:")
            df.printSchema()
            
            self.logger.info("Sample data:")
            df.show(5)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read from MinIO: {str(e)}")
            return None

    def transform_for_clickhouse(self: 'MinioToClickHouse', df: DataFrame) -> DataFrame:
        """Apply ClickHouse-optimized transformations."""
        
        self.logger.info("Applying ClickHouse-optimized transformations")

        cleaned_df = df.fillna({
            'energy_consumption': 15.0,
            'current_reading': 10.0,
            'power_factor': 0.95,
            'frequency': 50.0,
            'voltage': 220.0
        })

        cleaned_df = DataQualityChecker.validate_meter_data(cleaned_df)

        transformed_df = (cleaned_df
            .withColumn("timestamp", to_timestamp(col("timestamp")))
            .withColumn("date", to_date(col("timestamp")))
            .withColumn("year", year(col("timestamp")))
            .withColumn("month", month(col("timestamp")))
            .withColumn("day", dayofmonth(col("timestamp")))
            .withColumn("hour", hour(col("timestamp")))
            .withColumn("minute", minute(col("timestamp")))
            .withColumn("consumption_category",
                when(col("energy_consumption") < 10, "LOW")
                .when(col("energy_consumption") < 25, "MEDIUM")
                .otherwise("HIGH"))
            .withColumn("is_anomaly", 
                when(
                    (col("energy_consumption") > 50) | 
                    (col("voltage") < 200) | 
                    (col("voltage") > 250), 1
                ).otherwise(0))
            .withColumn("partition_date", to_date(col("timestamp")))
            .withColumn("processed_at", current_timestamp())
        )

        self.logger.info(f"Transformations completed - {transformed_df.count()} records")
        return transformed_df

    def create_clickhouse_aggregations(self: 'MinioToClickHouse', df: DataFrame) -> dict:
        """Create multiple aggregation levels optimized for ClickHouse analytics."""
        
        self.logger.info("Creating ClickHouse aggregations")
        
        hourly_agg = (df
            .groupBy("meter_id", "date", "hour", "partition_date")
            .agg(
                sum("energy_consumption").alias("total_energy_hourly"),
                avg("energy_consumption").alias("avg_energy_hourly"),
                avg("voltage").alias("avg_voltage_hourly"),
                avg("current_reading").alias("avg_current_hourly"),
                max("energy_consumption").alias("max_consumption_hourly"),
                min("energy_consumption").alias("min_consumption_hourly"),
                count("*").alias("record_count_hourly"),
                sum("is_anomaly").alias("anomaly_count_hourly")
            )
            .withColumn("aggregation_type", lit("HOURLY"))
        )

        daily_agg = (df
            .groupBy("meter_id", "date", "partition_date")
            .agg(
                sum("energy_consumption").alias("total_energy_daily"),
                avg("energy_consumption").alias("avg_energy_daily"),
                avg("voltage").alias("avg_voltage_daily"),
                avg("current_reading").alias("avg_current_daily"),
                max("energy_consumption").alias("max_consumption_daily"),
                min("energy_consumption").alias("min_consumption_daily"),
                stddev("energy_consumption").alias("std_energy_daily"),
                count("*").alias("record_count_daily"),
                sum("is_anomaly").alias("anomaly_count_daily")
            )
            .withColumn("aggregation_type", lit("DAILY"))
        )
        
        meter_agg = (df
            .groupBy("meter_id", "partition_date")
            .agg(
                sum("energy_consumption").alias("total_energy_meter"),
                avg("energy_consumption").alias("avg_energy_meter"),
                max("energy_consumption").alias("peak_consumption"),
                count("*").alias("total_readings"),
                approx_count_distinct("date").alias("active_days"),
                sum("is_anomaly").alias("total_anomalies")
            )
            .withColumn("aggregation_type", lit("METER_SUMMARY"))
        )

        aggregations = {
            'raw_data': df,
            'hourly_aggregates': hourly_agg,
            'daily_aggregates': daily_agg,
            'meter_aggregates': meter_agg
        }
        
        for agg_name, agg_df in aggregations.items():
            self.logger.info(f"   {agg_name}: {agg_df.count()} records")
        
        self.logger.info("ClickHouse aggregations created successfully")
        return aggregations

    def write_to_clickhouse(
        self: 'MinioToClickHouse',
        dataframes: dict[str, DataFrame]
    ) -> bool:
        """Write data to ClickHouse using JDBC with optimized settings."""
        
        try:
            config = configparser.ConfigParser()
            config.read(self.config_path)
            ch_config = config['clickhouse']
            
            jdbc_url = (
                f"jdbc:clickhouse://{ch_config['host']}:{ch_config['port']}/"
                f"{ch_config['database']}"
            )

            properties = {
                "user": ch_config['user'],
                "password": ch_config['password'],
                "driver": "com.clickhouse.jdbc.ClickHouseDriver",
                "batchsize": "100000",
                "numPartitions": "4",
                "socket_timeout": "300000",
                "connect_timeout": "10000",
                "rewriteBatchedStatements": "true",
                "use_server_time_zone": "false",
                "use_time_zone": "UTC"
            }

            raw_data_for_ch = dataframes['raw_data'].select(
                "meter_id", "timestamp", "date", "year", "month", "day", "hour", "minute",
                "energy_consumption", "voltage", "current_reading", 
                "power_factor", "frequency", "consumption_category",
                "is_anomaly", "partition_date", "processed_at"
            )
            
            self.logger.info(
                f"Writing {raw_data_for_ch.count()} records to smart_meter_raw table"
            )
            (raw_data_for_ch.write
                .mode("append")
                .option("createTableOptions", """
                    ENGINE = MergeTree()
                    PARTITION BY toYYYYMM(partition_date)
                    ORDER BY (meter_id, timestamp)
                    SETTINGS index_granularity = 8192
                """)
                .jdbc(
                    url=jdbc_url,
                    table="smart_meter_raw",
                    properties=properties
                )
            )

            for agg_name, agg_df in dataframes.items():
                if agg_name != 'raw_data':
                    self.logger.info(
                        f"Writing {agg_df.count()} "
                        f"records to meter_aggregates ({agg_name})"
                    )
                    
                    if agg_name == 'hourly_aggregates':
                        agg_for_write = agg_df.select(
                            "meter_id", "date", "hour", "partition_date",
                            "total_energy_hourly", "avg_energy_hourly", "avg_voltage_hourly",
                            "avg_current_hourly", "max_consumption_hourly", "min_consumption_hourly",
                            "record_count_hourly", "anomaly_count_hourly", "aggregation_type"
                        )
                    elif agg_name == 'daily_aggregates':
                        agg_for_write = agg_df.select(
                            "meter_id", "date", "partition_date",
                            "total_energy_daily", "avg_energy_daily", "avg_voltage_daily",
                            "avg_current_daily", "max_consumption_daily", "min_consumption_daily",
                            "std_energy_daily", "record_count_daily", "anomaly_count_daily",
                            "aggregation_type"
                        )
                    else:  # meter_aggregates
                        agg_for_write = agg_df.select(
                            "meter_id", "partition_date",
                            "total_energy_meter", "avg_energy_meter", "peak_consumption",
                            "total_readings", "active_days", "total_anomalies", "aggregation_type"
                        )
                    
                    (agg_for_write.write
                        .mode("append")
                        .jdbc(
                            url=jdbc_url,
                            table="meter_aggregates",
                            properties=properties
                        )
                    )

            self.logger.info("Successfully wrote all data to ClickHouse")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to write to ClickHouse: {str(e)}")
            return False

    def run(self: 'MinioToClickHouse') -> bool:
        """Execute the complete ClickHouse ETL pipeline."""
        
        try:
            self.initialize_spark()
            
            self.logger.info(
                f"Starting MinIO to ClickHouse ETL for date: "
                f"{self.processing_date}"
            )
            
            raw_df = self.read_from_minio()
            
            if raw_df is None or raw_df.count() == 0:
                self.logger.warning("No data found to process")
                return True
            
            self.logger.info(f"Raw data count: {raw_df.count()}")
            
            transformed_df = self.transform_for_clickhouse(raw_df)
            
            if transformed_df.count() == 0:
                self.logger.warning("No data passed transformations")
                return True
                
            self.logger.info(f"Transformed data count: {transformed_df.count()}")
            
            aggregations = self.create_clickhouse_aggregations(transformed_df)
            
            success = self.write_to_clickhouse(aggregations)
            
            if success:
                self.logger.info("MinIO to ClickHouse ETL completed successfully")
                for name, df in aggregations.items():
                    self.logger.info(f"   {name}: {df.count()} records")
            else:
                self.logger.error("ClickHouse ETL failed")
            
            return success
            
        except Exception as e:
            self.logger.error(f"ETL pipeline execution failed: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False

        finally:
            if self.spark:
                self.spark.stop()
                self.logger.info("Spark session stopped")


def main() -> None:
    parser = argparse.ArgumentParser(description='MinIO to ClickHouse Spark ETL Job')
    parser.add_argument('--config', required=True, help='Path to configuration file')
    parser.add_argument('--date', required=True, help='Processing date (YYYY-MM-DD)')
    parser.add_argument('--prod', action='store_true', help='Run in production mode')

    args: Namespace = parser.parse_args()  
    typed_args = cast(Arguments, vars(args))

    config_full_path = os.path.join(
        os.path.dirname(__file__),
        '..',
        'config',
        typed_args['config']
    )
    if not os.path.exists(config_full_path):
        print(f"Error: Config file not found: {config_full_path}")
        sys.exit(1)

    job = MinioToClickHouse(config_full_path, typed_args['date'])

    success = job.run()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
