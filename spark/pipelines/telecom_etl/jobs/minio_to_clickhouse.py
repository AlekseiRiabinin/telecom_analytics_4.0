"""
MinIO to ClickHouse Spark Job - Optimized for ClickHouse Hook.
"""

import sys
import os
import json
import logging
import argparse
import configparser
import requests
import concurrent.futures
from threading import Lock
from typing import TypedDict, cast
from argparse import Namespace
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame, Row
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
        """
        Optimized ClickHouse writer using parallel HTTP inserts.
        Fixed Spark Row object handling.
        """
        
        try:
            config = configparser.ConfigParser()
            config.read(self.config_path)
            ch_config = config['clickhouse']
            
            base_url = f"http://{ch_config['host']}:{ch_config['http_port']}"
            auth = (ch_config['user'], ch_config['password'])
            
            test_response = requests.post(
                base_url,
                params={'query': 'SELECT 1'},
                auth=auth,
                timeout=10
            )
            if test_response.status_code != 200:
                self.logger.error(
                    f"ClickHouse connection test failed: {test_response.text}"
                )
                return False

            total_inserted = 0
            lock = Lock()
            
            def convert_row_to_dict(row):
                """Convert Spark Row to Python dictionary."""

                try:
                    typed_row: Row = row 
                    return typed_row.asDict()

                except Exception as e:
                    return {'error': str(e), 'data': str(typed_row)}
           
            def insert_batch(
                table_name: str, batch_data: list, batch_id: int
            ) -> tuple[int, bool]:
                """Insert a batch of records using JSONEachRow format."""

                try:
                    if not batch_data:
                        return 0, True
                    
                    rows = []
                    for row in batch_data:
                        row_dict = convert_row_to_dict(row)
                        rows.append(json.dumps(row_dict, default=str))
                    
                    insert_data = "\n".join(rows)
                    
                    response = requests.post(
                        base_url,
                        params={
                            'query': (
                                f'INSERT INTO telecom_analytics.{table_name} '
                                f'FORMAT JSONEachRow'
                            ),
                            'input_format_skip_unknown_fields': 1
                        },
                        data=insert_data,
                        auth=auth,
                        headers={'Content-Type': 'text/plain'},
                        timeout=120,
                        verify=False
                    )
                    
                    if response.status_code == 200:
                        with lock:
                            nonlocal total_inserted
                            total_inserted += len(batch_data)
                        self.logger.debug(
                            f"Batch {batch_id}: {len(batch_data)} records → {table_name}"
                        )
                        return len(batch_data), True

                    else:
                        self.logger.error(f"Batch {batch_id} failed: {response.text}")
                        return 0, False
                        
                except Exception as e:
                    self.logger.error(f"Batch {batch_id} exception: {e}")
                    return 0, False
            
            for df_name, df in dataframes.items():
                record_count = df.count()
                if record_count == 0:
                    self.logger.info(f"No data to insert for {df_name}")
                    continue
                    
                table_name = self._get_table_name(df_name)
                self.logger.info(f"Inserting {record_count:,} records to {table_name}")
                
                # ~50K records per partition
                optimal_partitions = min(32, max(8, record_count // 50000))
                partitioned_df = df.repartition(optimal_partitions)
                
                batches = partitioned_df.rdd.glom().collect()
                self.logger.info(f"Processing {len(batches)} batches in parallel")
                
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=min(16, len(batches)),
                    thread_name_prefix=f"ch_{df_name}"
                ) as executor:
                    
                    future_to_batch = {
                        executor.submit(insert_batch, table_name, batch, i): i
                        for i, batch in enumerate(batches) if batch
                    }
                    
                    batch_results = []
                    for future in concurrent.futures.as_completed(future_to_batch):
                        batch_id = future_to_batch[future]
                        try:
                            count, success = future.result(timeout=180)
                            batch_results.append((batch_id, count, success))

                        except concurrent.futures.TimeoutError:
                            self.logger.error(f"Batch {batch_id} timed out")
                            batch_results.append((batch_id, 0, False))

                        except Exception as e:
                            self.logger.error(f"Batch {batch_id} failed: {e}")
                            batch_results.append((batch_id, 0, False))
                    
                    successful_batches = sum(1 for _, _, success in batch_results if success)
                    total_batches = len(batch_results)
                    inserted_in_df = sum(count for _, count, _ in batch_results)
                    
                    self.logger.info(
                        f"{df_name}: {successful_batches}/{total_batches} batches, "
                        f"{inserted_in_df:,}/{record_count:,} records"
                    )
                    
                    if successful_batches == 0:
                        self.logger.error(f"All batches failed for {df_name}")
                        return False
            
            self.logger.info(f"Total inserted across all tables: {total_inserted:,} records")
            return total_inserted > 0
            
        except Exception as e:
            self.logger.error(f"Write to ClickHouse failed: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False

    def _get_table_name(self, df_name: str) -> str:
        """Map DataFrame name to ClickHouse table name."""

        table_mapping = {
            'raw_data': 'smart_meter_raw',
            'hourly_aggregates': 'meter_aggregates',
            'daily_aggregates': 'meter_aggregates', 
            'meter_aggregates': 'meter_aggregates'
        }
        return table_mapping.get(df_name, df_name)

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
