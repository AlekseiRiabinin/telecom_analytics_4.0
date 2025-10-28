"""
Kafka to MinIO Spark Job
Reads from Kafka topic and writes to MinIO in Parquet format.
"""


import sys
import os
import logging
import argparse
import configparser
from typing import TypedDict, cast
from datetime import datetime
from argparse import Namespace
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from spark.pipelines.shared.utils.spark_utils import (
    SparkSessionManager,
    SchemaManager
)


current_dir = os.path.dirname(os.path.abspath(__file__))
spark_pipelines_path = os.path.join(current_dir, '..', '..', '..')
if spark_pipelines_path not in sys.path:
    sys.path.insert(0, spark_pipelines_path)

try:
    from shared.utils.spark_utils import SparkSessionManager, SchemaManager
    print("Successfully imported SparkSessionManager and SchemaManager")
except ImportError as e:
    print(f"Import error: {e}")
    print(f"Python path: {sys.path}")
    raise


class Arguments(TypedDict):
    config: str
    date: str | None


class KafkaToMinio:
    """Stream from Kafka to MinIO."""
    
    def __init__(
        self: 'KafkaToMinio',
        config_path: str,
        processing_date: datetime=None
    ) -> None:
        self.config_path = config_path
        self.processing_date = (
            processing_date or datetime.now().strftime('%Y-%m-%d')
        )
        self.spark = None
        self.logger = self.setup_logging()

    def setup_logging(self: 'KafkaToMinio') -> logging:
        """Initialize Logger."""

        logging.basicConfig(level=logging.INFO)
        return logging.getLogger('KafkaToMinio')

    def initialize_spark(self: 'KafkaToMinio') -> SparkSession:
        """Initialize Spark session."""

        self.logger.info("Initializing Spark session")
        self.spark = SparkSessionManager.create_spark_session(
            "KafkaToMinio", 
            self.config_path
        )

    def create_sample_data(self: 'KafkaToMinio') -> DataFrame:
        """Create data for testing."""

        self.logger.info("Creating sample data for testing")
        
        sample_data = [
            ("METER_001", "2024-01-15 10:00:00", 15.75, 220.5, 7.1, 0.95, 50.0),
            ("METER_002", "2024-01-15 10:01:00", 22.30, 219.8, 10.2, 0.92, 49.9),
            ("METER_003", "2024-01-15 10:02:00", 18.45, 221.2, 8.3, 0.98, 50.1),
            ("METER_001", "2024-01-15 11:00:00", 14.20, 220.8, 6.4, 0.96, 50.1),
            ("METER_002", "2024-01-15 11:01:00", 20.15, 219.9, 9.1, 0.93, 49.9),
        ]
        
        columns = [
            "meter_id", "timestamp", "energy_consumption",
            "voltage", "current_reading", "power_factor", "frequency"
        ]
        df = self.spark.createDataFrame(sample_data, columns)
        
        self.logger.info(f"Created sample data with {df.count()} records")
        return df

    def write_to_minio(self: 'KafkaToMinio', df: DataFrame) -> bool:
        """Write data to MinIO in Parquet format."""

        try:
            output_path = (
                f"/tmp/telecom_data/smart_meter_data/date={self.processing_date}"
            )

            self.logger.info(f"Writing data to: {output_path}")

            (df.write
                .mode("overwrite")
                .parquet(output_path))
            
            self.logger.info(f"Successfully wrote data to {output_path}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to write data: {str(e)}")
            return False
    
    def run(self: 'KafkaToMinio') -> bool:
        """Execute the simple pipeline"""

        try:
            self.initialize_spark()
            
            # Create sample data (instead of reading from Kafka)
            df = self.create_sample_data()
            
            # Show the data
            self.logger.info("Sample data:")
            df.show()
            
            # Write to "MinIO" (local filesystem for testing)
            success = self.write_to_minio(df)
            
            if success:
                self.logger.info("Kafka to MinIO pipeline completed successfully")                
            else:
                self.logger.error("Pipeline failed")
            
            return success
            
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

    def read_from_kafka_prod(self: 'KafkaToMinio') -> DataFrame:
        """Read from Kafka in production."""

        try:
            config = configparser.ConfigParser()
            config.read(self.config_path)

            kafka_config = config['kafka']

            df = (self.spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'])
                .option("subscribe", kafka_config['topic'])
                .option("startingOffsets", "latest")
                .load()
            )
            
            schema = SchemaManager.get_smart_meter_schema()
            json_df = (df
                .select(col("value").cast("string").alias("json_data"))
                .select(from_json(col("json_data"), schema).alias("data"))
                .select("data.*")
            )
            
            return json_df
            
        except Exception as e:
            self.logger.error(f"Failed to read from Kafka: {str(e)}")
            return None

    def write_to_minio_prod(self: 'KafkaToMinio', df: DataFrame) -> bool:
        """Write data to MinIO in production."""

        try:
            config = configparser.ConfigParser()
            config.read(self.config_path)
            minio_config = config['minio']

            output_path = (
                f"s3a://{minio_config['raw_bucket']}"
                f"/smart_meter_data/date={self.processing_date}"
            )

            self.logger.info(f"Writing data to MinIO: {output_path}")
            
            (df.write
                .mode("append")
                .parquet(output_path))
            
            self.logger.info(f"Successfully wrote data to MinIO: {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to write to MinIO: {str(e)}")
            return False

    def run_prod(self: 'KafkaToMinio') -> bool:
        """Execute the production pipeline."""

        try:
            self.initialize_spark()
            
            df = self.read_from_kafka_prod()
            if df is None:
                self.logger.error("Failed to read from Kafka")
                return False
            
            self.logger.info("Data from Kafka:")
            df.show()
            
            success = self.write_to_minio_prod(df)
            
            if success:
                self.logger.info("Kafka to MinIO pipeline completed successfully")
            else:
                self.logger.error("Pipeline failed")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Production pipeline execution failed: {str(e)}")
            return False

############################################################################
############################################################################


def main() -> None:
    parser = argparse.ArgumentParser(description='Kafka to MinIO Spark Job')
    parser.add_argument('--config', required=True, help='Path to config file')
    parser.add_argument('--date', help='Processing date (YYYY-MM-DD)')
    
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
    
    job = KafkaToMinio(config_full_path, typed_args['date'])
    success = job.run()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
