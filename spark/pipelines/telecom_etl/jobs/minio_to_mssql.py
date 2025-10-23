"""
Simple MinIO to MSSQL Spark Job
Reads from MinIO, transforms data, and demonstrates MSSQL write
"""

import argparse
import logging
import sys
import os
from datetime import datetime

# Fix the import path - go up 3 levels from jobs directory to reach shared/utils
project_root = os.path.join(os.path.dirname(__file__), '..', '..', '..', '..')
shared_utils_path = os.path.join(project_root, 'spark', 'pipelines', 'shared', 'utils')
sys.path.insert(0, shared_utils_path)

try:
    from spark_utils import SparkSessionManager, DataQualityChecker
    print(f"✅ Successfully imported from: {shared_utils_path}")
except ImportError as e:
    print(f"❌ Import failed from {shared_utils_path}: {e}")
    print(f"Current Python path: {sys.path}")
    sys.exit(1)

from pyspark.sql.functions import *

class SimpleMinioToMSSQL:
    """Simple version: Read from MinIO, transform, prepare for MSSQL"""
    
    def __init__(self, config_path, processing_date):
        self.config_path = config_path
        self.processing_date = processing_date
        self.spark = None
        self.logger = self.setup_logging()
        
    def setup_logging(self):
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger('SimpleMinioToMSSQL')
    
    def initialize_spark(self):
        """Initialize Spark session"""
        self.logger.info("Initializing Spark session")
        self.spark = SparkSessionManager.create_spark_session(
            "SimpleMinioToMSSQL", 
            self.config_path
        )
    
    def read_from_minio(self):
        """Read data from MinIO (local filesystem for testing)"""
        try:
            input_path = f"/tmp/telecom_data/smart_meter_data/date={self.processing_date}"
            
            self.logger.info(f"Reading from: {input_path}")
            
            df = self.spark.read.parquet(input_path)
            
            record_count = df.count()
            self.logger.info(f"Successfully read {record_count} records from MinIO")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read from MinIO: {str(e)}")
            return None
    
    def transform_data(self, df):
        """Apply simple transformations"""
        self.logger.info("Applying data transformations")
        
        # Data quality check
        cleaned_df = DataQualityChecker.validate_meter_data(df)
        
        # Basic transformations
        transformed_df = cleaned_df \
            .withColumn("timestamp", to_timestamp(col("timestamp"))) \
            .withColumn("date", to_date(col("timestamp"))) \
            .withColumn("hour", hour(col("timestamp"))) \
            .withColumn("consumption_category",
                       when(col("energy_consumption") < 10, "LOW")
                       .when(col("energy_consumption") < 25, "MEDIUM")
                       .otherwise("HIGH")) \
            .withColumn("processed_at", current_timestamp())
        
        self.logger.info("Data transformations completed")
        return transformed_df
    
    def create_aggregations(self, df):
        """Create simple aggregations for reporting"""
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
    
    def demonstrate_mssql_write(self, dataframes):
        """Demonstrate how data would be written to MSSQL (just show the data for now)"""
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
        
        # In production, we would actually write to MSSQL here:
        # dataframes['cleaned_data'].write.jdbc(...)
        # dataframes['daily_aggregates'].write.jdbc(...)
    
    def run(self):
        """Execute the simple pipeline"""
        try:
            self.initialize_spark()
            
            # Read from MinIO
            raw_df = self.read_from_minio()
            
            if raw_df is None or raw_df.count() == 0:
                self.logger.warning("No data found to process")
                return True
            
            # Show raw data
            self.logger.info("Raw data from MinIO:")
            raw_df.show()
            
            # Transform data
            transformed_df = self.transform_data(raw_df)
            
            # Show transformed data
            self.logger.info("Transformed data:")
            transformed_df.select(
                "meter_id", "timestamp", "energy_consumption", 
                "consumption_category", "processed_at"
            ).show()
            
            # Create aggregations
            daily_aggregates = self.create_aggregations(transformed_df)
            
            # Demonstrate MSSQL write
            dataframes = {
                'cleaned_data': transformed_df,
                'daily_aggregates': daily_aggregates
            }
            self.demonstrate_mssql_write(dataframes)
            
            self.logger.info("Simple MinIO to MSSQL pipeline completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {str(e)}")
            return False
        finally:
            if self.spark:
                self.spark.stop()
                self.logger.info("Spark session stopped")

def main():
    parser = argparse.ArgumentParser(description='Simple MinIO to MSSQL Spark Job')
    parser.add_argument('--config', required=True, help='Path to configuration file')
    parser.add_argument('--date', required=True, help='Processing date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Validate config file exists
    config_full_path = os.path.join(os.path.dirname(__file__), '..', 'config', args.config)
    if not os.path.exists(config_full_path):
        print(f"Error: Config file not found: {config_full_path}")
        sys.exit(1)
    
    # Validate date format
    try:
        datetime.strptime(args.date, '%Y-%m-%d')
    except ValueError:
        print(f"Error: Invalid date format: {args.date}. Use YYYY-MM-DD")
        sys.exit(1)
    
    job = SimpleMinioToMSSQL(config_full_path, args.date)
    success = job.run()
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
