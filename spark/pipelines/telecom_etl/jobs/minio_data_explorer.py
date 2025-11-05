"""
MinIO Data Explorer Spark Job
Comprehensive data exploration and quality analysis for MinIO data.
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
from spark.pipelines.shared.utils.spark_utils import SparkSessionManager
from pyspark.sql.functions import *


class Arguments(TypedDict):
    config: str
    date: str | None
    prod: bool


class MinioDataExplorer:
    """Comprehensive data exploration and quality analysis for MinIO data."""
    
    def __init__(
        self: 'MinioDataExplorer',
        config_path: str,
        processing_date: datetime = None
    ) -> None:
        self.config_path = config_path
        self.processing_date = processing_date
        self.spark = None
        self.logger = self.setup_logging()

    def setup_logging(self: 'MinioDataExplorer') -> logging:
        """Setup logging."""

        logging.basicConfig(level=logging.INFO)
        return logging.getLogger('MinioDataExplorer')
    
    def initialize_spark(self: 'MinioDataExplorer') -> SparkSession:
        """Initialize Spark session."""

        self.logger.info("Initializing Spark session for data exploration")
        self.spark = SparkSessionManager.create_spark_session(
            "MinioDataExplorer", 
            self.config_path
        )

        self.spark.sparkContext.setLogLevel("WARN")
        return self.spark

    def read_from_minio(self: 'MinioDataExplorer') -> DataFrame:
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


############################################################################
################################### PROD ###################################
############################################################################

    def read_from_minio_prod(self: 'MinioDataExplorer') -> DataFrame:
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

            try:
                base_path = f"s3a://{minio_config['raw_bucket']}/smart_meter_data"
                self.logger.info(f"Trying base path: {base_path}")
                df = self.spark.read.parquet(base_path)
                record_count = df.count()
                self.logger.info(f"Successfully read {record_count} records from base path")
                return df

            except Exception as e2:
                self.logger.error(f"Failed to read from base path: {str(e2)}")
                return None

    def analyze_basic_statistics(self: 'MinioDataExplorer', df: DataFrame) -> None:
        """Analyze basic statistics and data structure."""

        self.logger.info("=== BASIC DATA STATISTICS ===")
        
        total_count = df.count()
        self.logger.info(f"Total records: {total_count}")
        
        if total_count == 0:
            self.logger.warning("No data found to analyze!")
            return
        
        self.logger.info("Data Schema:")
        df.printSchema()
        
        self.logger.info("Sample data (first 20 rows):")
        df.show(20, truncate=False)
        
        numeric_columns = [
            'voltage', 'current_reading',
            'energy_consumption', 'power_factor', 'frequency'
        ]
        
        self.logger.info("\n=== NUMERIC COLUMNS ANALYSIS ===")
        for col_name in numeric_columns:
            if col_name in df.columns:
                self.logger.info(f"\n--- Analysis for {col_name} ---")
                
                stats = (df
                    .select(
                        count(col_name).alias('count'),
                        mean(col_name).alias('mean'),
                        stddev(col_name).alias('stddev'),
                        min(col_name).alias('min'),
                        max(col_name).alias('max'),
                        sum(when(col(col_name).isNull(), 1).otherwise(0)).alias('null_count'),
                        sum(when(isnan(col(col_name)), 1).otherwise(0)).alias('nan_count')
                    )
                    .collect()[0]
                )
                
                self.logger.info(f"Total values: {stats['count']}")
                self.logger.info(
                    f"NULL values: {stats['null_count']} "
                    f"({(stats['null_count']/total_count)*100:.2f}%)"
                )
                self.logger.info(
                    f"NaN values: {stats['nan_count']} "
                    f"({(stats['nan_count']/total_count)*100:.2f}%)"
                )
                
                if stats['mean'] is not None:
                    self.logger.info(f"Mean: {stats['mean']:.4f}")
                    self.logger.info(f"StdDev: {stats['stddev']:.4f}")
                    self.logger.info(f"Min: {stats['min']}")
                    self.logger.info(f"Max: {stats['max']}")
                else:
                    self.logger.info("Mean: No data (all NULL)")
                    self.logger.info("StdDev: No data (all NULL)")
                    self.logger.info("Min: No data (all NULL)")
                    self.logger.info("Max: No data (all NULL)")
                
                self.logger.info("Detailed statistics:")
                df.select(col_name).describe().show()
                
                if stats['count'] > 0:
                    try:
                        quantiles = df.approxQuantile(
                            col_name, [0.0, 0.25, 0.5, 0.75, 1.0], 0.05
                        )
                        self.logger.info(
                            f"Quantiles [0%, 25%, 50%, 75%, 100%]: "
                            f"{[f'{q:.4f}' for q in quantiles]}"
                        )

                    except Exception as e:
                        self.logger.warning(
                            f"Could not calculate quantiles for {col_name}: {e}"
                        )

                else:
                    self.logger.info("Quantiles: No data available (all NULL)")

    def analyze_string_columns(self: 'MinioDataExplorer', df: DataFrame) -> None:
        """Analyze string columns."""

        self.logger.info("\n=== STRING COLUMNS ANALYSIS ===")
        
        string_columns = ['meter_id']
        total_count = df.count()
        
        for col_name in string_columns:
            if col_name in df.columns:
                self.logger.info(f"\n--- Analysis for {col_name} ---")
                
                distinct_count = df.select(col_name).distinct().count()
                null_count = df.filter(col(col_name).isNull()).count()
                empty_count = df.filter(trim(col(col_name)) == "").count()
                
                self.logger.info(f"Distinct values: {distinct_count}")
                self.logger.info(
                    f"NULL values: {null_count} "
                    f"({(null_count/total_count)*100:.2f}%)"
                )
                self.logger.info(
                    f"Empty values: {empty_count} "
                    f"({(empty_count/total_count)*100:.2f}%)"
                )

                self.logger.info("Top 10 most frequent values:")
                (df.groupBy(col_name)
                   .count()
                   .orderBy(desc("count"))
                   .show(10, truncate=False))

    def analyze_timestamp_columns(self: 'MinioDataExplorer', df: DataFrame) -> None:
        """Analyze timestamp columns."""

        self.logger.info("\n=== TIMESTAMP COLUMNS ANALYSIS ===")
        
        if 'timestamp' in df.columns:
            timestamp_stats = (df
                .select(
                    min('timestamp').alias('min_timestamp'),
                    max('timestamp').alias('max_timestamp'),
                    count('timestamp').alias('total_count'),
                    sum(when(col('timestamp').isNull(), 1).otherwise(0)).alias('null_count')
                )
                .collect()[0]
            )
            
            self.logger.info(
                f"Timestamp range: {timestamp_stats['min_timestamp']} "
                f"to {timestamp_stats['max_timestamp']}"
            )
            self.logger.info(f"NULL timestamps: {timestamp_stats['null_count']}")

    def validate_data_quality_rules(self: 'MinioDataExplorer', df: DataFrame) -> None:
        """Validate data against quality rules and show failure analysis."""

        self.logger.info("\n=== DATA QUALITY VALIDATION ===")
        
        total_count = df.count()
        
        if total_count == 0:
            self.logger.warning("No data to validate")
            return
        
        validation_checks = [
            (
                "voltage BETWEEN 200-250",
                (col("voltage") >= 200) & (col("voltage") <= 250)
            ),
            (
                "current_reading BETWEEN 0-100",
                (col("current_reading") >= 0) & (col("current_reading") <= 100)
            ),
            (
                "energy_consumption > 0", 
                col("energy_consumption") > 0
            ),
            (
                "meter_id NOT NULL", 
                col("meter_id").isNotNull()
            ),
            (
                "timestamp NOT NULL", 
                col("timestamp").isNotNull()
            )
        ]
        
        self.logger.info("Individual rule validation:")
        for check_name, condition in validation_checks:
            passed_count = df.filter(condition).count()
            percentage = (passed_count / total_count) * 100
            self.logger.info(
                f"  {check_name}: {passed_count}/{total_count} "
                f"({percentage:.2f}%)"
            )
        
        combined_condition = (
            (col("voltage") >= 200) & (col("voltage") <= 250) &
            (col("current_reading") >= 0) & (col("current_reading") <= 100) &
            (col("energy_consumption") > 0) &
            col("meter_id").isNotNull() &
            col("timestamp").isNotNull()
        )
        
        valid_count = df.filter(combined_condition).count()
        self.logger.info(
            f"\nRecords passing ALL validation rules: "
            f"{valid_count}/{total_count} ({(valid_count/total_count)*100:.2f}%)"
        )
        
        self.analyze_validation_failures(df)

    def analyze_validation_failures(self: 'MinioDataExplorer', df: DataFrame) -> None:
        """Simple validation failure analysis without complex operations."""
        
        self.logger.info("\n=== VALIDATION FAILURE ANALYSIS ===")
   
        null_energy = df.filter(col("energy_consumption").isNull()).count()
        null_voltage = df.filter(col("voltage").isNull()).count()
        null_meter = df.filter(col("meter_id").isNull()).count()
        null_timestamp = df.filter(col("timestamp").isNull()).count()
        
        self.logger.info("Null value analysis:")
        self.logger.info(f"  NULL energy_consumption: {null_energy} records")
        self.logger.info(f"  NULL voltage: {null_voltage} records")
        self.logger.info(f"  NULL meter_id: {null_meter} records")
        self.logger.info(f"  NULL timestamp: {null_timestamp} records")
        
        if (
            null_energy == 0 and 
            null_voltage == 0 and 
            null_meter == 0 and 
            null_timestamp == 0
        ):
            self.logger.info("✓ Confirmed: No NULL values found in critical columns")
            self.logger.info("✓ Data quality validation: PASSED")

    def generate_summary_report(self: 'MinioDataExplorer', df: DataFrame) -> None:
        """Generate comprehensive summary report."""

        self.logger.info("\n" + "="*60)
        self.logger.info("DATA EXPLORATION SUMMARY REPORT")
        self.logger.info("="*60)
        
        total_count = df.count()
        self.logger.info(f"Total Records: {total_count}")
        
        if total_count == 0:
            self.logger.info("No data available for analysis")
            return

        completeness_checks = []
        for col_name in df.columns:
            non_null_count = df.filter(col(col_name).isNotNull()).count()
            completeness = (non_null_count / total_count) * 100
            completeness_checks.append((col_name, completeness))
        
        self.logger.info("\nDATA COMPLETENESS:")
        for col_name, completeness in completeness_checks:
            self.logger.info(f"  {col_name}: {completeness:.1f}%")
        
        valid_count = (df
            .filter(
                (col("voltage") >= 200) & (col("voltage") <= 250) &
                (col("current_reading") >= 0) & (col("current_reading") <= 100) &
                (col("energy_consumption") > 0) &
                col("meter_id").isNotNull() &
                col("timestamp").isNotNull()
            )
            .count()
        )

        quality_score = (valid_count / total_count) * 100
        self.logger.info(f"\nDATA QUALITY SCORE: {quality_score:.1f}%")
        
        if quality_score < 80:
            self.logger.warning(
                "LOW DATA QUALITY - Consider reviewing data source or validation rules"
            )
        elif quality_score < 95:
            self.logger.info(
                "MODERATE DATA QUALITY - Some records require attention"
            )
        else:
            self.logger.info(
                "HIGH DATA QUALITY - Data meets quality standards"
            )
        
        self.logger.info("="*60)

    def run_prod(self: 'MinioDataExplorer') -> bool:
        """Execute the production data exploration pipeline."""

        try:
            self.initialize_spark()

            raw_df = self.read_from_minio_prod()
            
            if raw_df is None or raw_df.count() == 0:
                self.logger.warning("No data found to analyze")
                return True

            self.analyze_basic_statistics(raw_df)
            self.analyze_string_columns(raw_df)
            self.analyze_timestamp_columns(raw_df)
            self.validate_data_quality_rules(raw_df)
            self.generate_summary_report(raw_df)
            
            self.logger.info("Data exploration completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Data exploration failed: {str(e)}")
            return False

        finally:
            if self.spark:
                self.spark.stop()
                self.logger.info("Spark session stopped")

    def run(self: 'MinioDataExplorer') -> bool:
        """Execute the development data exploration pipeline."""

        try:
            self.initialize_spark()

            raw_df = self.read_from_minio()
            
            if raw_df is None or raw_df.count() == 0:
                self.logger.warning("No data found to analyze")
                return True

            self.analyze_basic_statistics(raw_df)
            self.validate_data_quality_rules(raw_df)
            
            self.logger.info("Development data exploration completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Data exploration failed: {str(e)}")
            return False

        finally:
            if self.spark:
                self.spark.stop()
                self.logger.info("Spark session stopped")

############################################################################
############################################################################


def main() -> None:
    parser = argparse.ArgumentParser(description='MinIO Data Explorer Spark Job')
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

    job = MinioDataExplorer(config_full_path, typed_args['date'])

    if typed_args['prod']:
        print("Running in PRODUCTION mode (comprehensive data exploration)")
        success = job.run_prod()
    else:
        print("Running in DEVELOPMENT mode (basic data exploration)")
        success = job.run()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
