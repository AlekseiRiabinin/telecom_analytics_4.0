"""
MinIO to MSSQL Spark Job
Reads from MinIO, transforms data, and demonstrates MSSQL write.
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


class Arguments(TypedDict):
    config: str
    date: str | None
    prod: bool


class MinioToMSSQL:
    """Read from MinIO, transform, prepare for MSSQL."""
    
    def __init__(
        self: 'MinioToMSSQL',
        config_path: str,
        processing_date: datetime=None
    ) -> None:
        self.config_path = config_path
        self.processing_date = processing_date
        self.spark = None
        self.logger = self.setup_logging()

    def setup_logging(self: 'MinioToMSSQL') -> logging:
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger('MinioToMSSQL')
    
    def initialize_spark(self: 'MinioToMSSQL') -> SparkSession:
        """Initialize Spark session."""

        self.logger.info("Initializing Spark session")
        self.spark = SparkSessionManager.create_spark_session(
            "MinioToMSSQL", 
            self.config_path
        )

    def read_from_minio(self: 'MinioToMSSQL') -> DataFrame:
        """Read data from MinIO (local filesystem for testing)."""
        try:
            input_path = (
                f"/tmp/telecom_data/smart_meter_data/date={self.processing_date}"
            )
            
            self.logger.info(f"Reading from: {input_path}")
            
            df = self.spark.read.parquet(input_path)
            
            record_count = df.count()
            self.logger.info(f"Successfully read {record_count} records from MinIO")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read from MinIO: {str(e)}")
            return None

    def debug_data_quality(self: 'MinioToMSSQL', df: DataFrame) -> None:
        """Debug data quality issues."""

        self.logger.info("=== DATA QUALITY DEBUG ===")
        
        for column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            total_count = df.count()
            self.logger.info(
                f"Column {column}: {null_count}/{total_count} "
                f"NULL values ({null_count / total_count * 100:.1f}%)"
            )
        
        self.logger.info("Sample of records with NULL energy_consumption:")

        (df.filter(col("energy_consumption").isNull())
            .select("meter_id", "timestamp", "energy_consumption", "voltage")
            .show(5))

        self.logger.info("=== END DATA QUALITY DEBUG ===")

    def transform_data(self: 'MinioToMSSQL', df: DataFrame) -> DataFrame:
        """Apply simple transformations."""

        self.logger.info("Applying data transformations")

        self.logger.info("Handling NULL values in source data")
        cleaned_df = df.fillna({
            'energy_consumption': 0.0,
            'current_reading': 0.0,
            'power_factor': 0.9,
            'frequency': 50.0
        })
        
        self.logger.info("Data after NULL handling:")
        self.debug_data_quality(cleaned_df)
        
        self.logger.info("Applying data quality validation")
        cleaned_df = DataQualityChecker.validate_meter_data(cleaned_df)

        transformed_df = (cleaned_df
            .withColumn("timestamp", to_timestamp(col("timestamp")))
            .withColumn("date", to_date(col("timestamp")))
            .withColumn("hour", hour(col("timestamp")))
            .withColumn("consumption_category",
                when(col("energy_consumption") < 10, "LOW")
                .when(col("energy_consumption") < 25, "MEDIUM")
                .otherwise("HIGH"))
            .withColumn("processed_at", current_timestamp())
        )

        self.logger.info("Data transformations completed")
        return transformed_df
    
    def create_aggregations(self: 'MinioToMSSQL', df: DataFrame) -> DataFrame:
        """Create aggregations for reporting."""

        self.logger.info("Creating daily aggregations")

        daily_aggregates = df.groupBy("meter_id", "date").agg(
            sum("energy_consumption").alias("total_energy"),
            avg("voltage").alias("avg_voltage"),
            avg("current_reading").alias("avg_current"),
            max("energy_consumption").alias("max_consumption"),
            count("*").alias("record_count")
        ).withColumn("created_at", current_timestamp())

        self.logger.info("Aggregations created successfully")
        return daily_aggregates

    def demonstrate_mssql_write(
        self: 'MinioToMSSQL',
        dataframes: dict[str, DataFrame]
    ) -> None:
        """Demonstrate how data would be written to MSSQL."""
    
        self.logger.info("Demonstrating MSSQL write operations")
        
        print("\n" + "="*50)
        print("DATA READY FOR MSSQL WRITE")
        print("="*50)
        
        print("\n1. Cleaned Meter Data (would write to smart_meter_data table):")
        dataframes['cleaned_data'].select(
            "meter_id", "timestamp", "energy_consumption", "voltage", 
            "current_reading", "power_factor", "frequency"
        ).show()
        
        print("\n2. Daily Aggregates (would write to daily_energy_consumption table):")
        dataframes['daily_aggregates'].show()
        
        print("\n" + "="*50)
        self.logger.info("MSSQL write demonstration completed")
  
    def run(self: 'MinioToMSSQL') -> bool:
        """Execute the pipeline."""

        try:
            self.initialize_spark()
            
            raw_df = self.read_from_minio()
            
            if raw_df is None or raw_df.count() == 0:
                self.logger.warning("No data found to process")
                return True
            
            self.logger.info("Raw data from MinIO:")
            raw_df.show()
            
            transformed_df = self.transform_data(raw_df)
            
            self.logger.info("Transformed data:")
            transformed_df.select(
                "meter_id", "timestamp", "energy_consumption", 
                "consumption_category", "processed_at"
            ).show()
            
            daily_aggregates = self.create_aggregations(transformed_df)
            
            dataframes = {
                'cleaned_data': transformed_df,
                'daily_aggregates': daily_aggregates
            }
            self.demonstrate_mssql_write(dataframes)
            
            self.logger.info("MinIO to MSSQL pipeline completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {str(e)}")
            return False

        finally:
            if self.spark:
                self.spark.stop()
                self.logger.info("Spark session stopped")


############################################################################
################################### PROD ###################################
############################################################################

    def read_from_minio_prod(self: 'MinioToMSSQL') -> DataFrame:
        """Read data from MinIO in production."""

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
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read from MinIO: {str(e)}")
            return None

    def write_to_mssql_prod(
        self: 'MinioToMSSQL',
        dataframes: dict[str, DataFrame]
    ) -> bool:
        """Write data to MSSQL in production."""

        try:
            config = configparser.ConfigParser()
            config.read(self.config_path)
            mssql_config = config['mssql']

            jdbc_url = (
                f"jdbc:sqlserver://{mssql_config['host']}:{mssql_config['port']};"
                f"databaseName={mssql_config['database']};"
                f"encrypt=true;trustServerCertificate=true"
            )
            
            properties = {
                "user": mssql_config['user'],
                "password": mssql_config['password'],
                "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            }

            cleaned_data_for_db = dataframes['cleaned_data'].select(
                "meter_id", "timestamp", "energy_consumption", "voltage",
                "current_reading", "power_factor", "frequency"
            )

            self.logger.info(
                f"Writing {cleaned_data_for_db.count()} "
                f"records to smart_meter_data table"
            )
            (cleaned_data_for_db.write
                .jdbc(
                    url=jdbc_url,
                    table="smart_meter_data",
                    mode="append",
                    properties=properties
                )
            )

            daily_aggregates_for_db = dataframes['daily_aggregates'].select(
                "meter_id", "date", "total_energy", "avg_voltage", 
                "avg_current", "max_consumption", "record_count", "created_at"
            )

            self.logger.info(
                f"Writing {daily_aggregates_for_db.count()} "
                f"records to daily_energy_consumption table"
            )
            (daily_aggregates_for_db.write
                .jdbc(
                    url=jdbc_url,
                    table="daily_energy_consumption",
                    mode="overwrite",
                    properties=properties
                )
            )

            self.logger.info("Successfully wrote data to MSSQL")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to write to MSSQL: {str(e)}")
            return False

    def run_prod(self: 'MinioToMSSQL') -> bool:
        """Execute the production pipeline."""

        try:
            self.initialize_spark()

            raw_df = self.read_from_minio_prod()
            
            if raw_df is None or raw_df.count() == 0:
                self.logger.warning("No data found to process")
                return True

            self.debug_data_quality(raw_df)
            
            self.logger.info("Sample of raw data:")
            (raw_df
                .select(
                    "meter_id", "timestamp", "energy_consumption", 
                    "voltage", "current_reading"
                )
                .show(10))
            
            transformed_df = self.transform_data(raw_df)

            if transformed_df.count() == 0:
                self.logger.warning("No data passed transformations and quality checks")
                return True
                
            self.logger.info(f"Transformed data - Count: {transformed_df.count()}")
            self.logger.info("Sample of transformed data:")
            (transformed_df
                .select(
                    "meter_id", "timestamp", "energy_consumption", 
                    "consumption_category", "processed_at"
                )
                .show(10))
            
            daily_aggregates = self.create_aggregations(transformed_df)
            
            dataframes = {
                'cleaned_data': transformed_df,
                'daily_aggregates': daily_aggregates
            }
            
            success = self.write_to_mssql_prod(dataframes)
            
            if success:
                self.logger.info("MinIO to MSSQL pipeline completed successfully")
            else:
                self.logger.error("Production pipeline failed")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {str(e)}")
            return False

        finally:
            if self.spark:
                self.spark.stop()
                self.logger.info("Spark session stopped")

############################################################################
############################################################################


def main() -> None:
    parser = argparse.ArgumentParser(description='MinIO to MSSQL Spark Job')
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

    job = MinioToMSSQL(config_full_path, typed_args['date'])

    if typed_args['prod']:
        print("Running in PRODUCTION mode (reading from MinIO, writing to MSSQL)")
        success = job.run_prod()
    else:
        print(
            f"Running in DEVELOPMENT mode "
            f"(reading from local filesystem, demonstrating MSSQL write)"
        )
        success = job.run()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
