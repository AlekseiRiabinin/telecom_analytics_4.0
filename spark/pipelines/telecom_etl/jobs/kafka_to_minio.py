#!/usr/bin/env python3
"""
Simple Kafka to MinIO Spark Job
Reads from Kafka topic and writes to MinIO in Parquet format
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
    from spark_utils import SparkSessionManager, SchemaManager
    print(f"✅ Successfully imported from: {shared_utils_path}")
except ImportError as e:
    print(f"❌ Import failed from {shared_utils_path}: {e}")
    print(f"Current Python path: {sys.path}")
    sys.exit(1)

from pyspark.sql.functions import *

class SimpleKafkaToMinio:
    """Simple version: Stream from Kafka to MinIO"""
    
    def __init__(self, config_path, processing_date=None):
        self.config_path = config_path
        self.processing_date = processing_date or datetime.now().strftime('%Y-%m-%d')
        self.spark = None
        self.logger = self.setup_logging()
        
    def setup_logging(self):
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger('SimpleKafkaToMinio')
    
    def initialize_spark(self):
        """Initialize Spark session"""
        self.logger.info("Initializing Spark session")
        self.spark = SparkSessionManager.create_spark_session(
            "SimpleKafkaToMinio", 
            self.config_path
        )
    
    def create_sample_data(self):
        """Create sample data for testing (since we might not have Kafka running locally)"""
        self.logger.info("Creating sample data for testing")
        
        sample_data = [
            ("METER_001", "2024-01-15 10:00:00", 15.75, 220.5, 7.1, 0.95, 50.0),
            ("METER_002", "2024-01-15 10:01:00", 22.30, 219.8, 10.2, 0.92, 49.9),
            ("METER_003", "2024-01-15 10:02:00", 18.45, 221.2, 8.3, 0.98, 50.1),
            ("METER_001", "2024-01-15 11:00:00", 14.20, 220.8, 6.4, 0.96, 50.1),
            ("METER_002", "2024-01-15 11:01:00", 20.15, 219.9, 9.1, 0.93, 49.9),
        ]
        
        columns = ["meter_id", "timestamp", "energy_consumption", "voltage", "current_reading", "power_factor", "frequency"]
        df = self.spark.createDataFrame(sample_data, columns)
        
        self.logger.info(f"Created sample data with {df.count()} records")
        return df
    
    def write_to_minio(self, df):
        """Write data to MinIO in Parquet format"""
        try:
            # For now, we'll write to local file system for testing
            # In production, this would be MinIO
            output_path = f"/tmp/telecom_data/smart_meter_data/date={self.processing_date}"
            
            self.logger.info(f"Writing data to: {output_path}")
            
            df.write \
                .mode("overwrite") \
                .parquet(output_path)
            
            self.logger.info(f"Successfully wrote data to {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to write data: {str(e)}")
            return False
    
    def run(self):
        """Execute the simple pipeline"""
        try:
            self.initialize_spark()
            
            # Create sample data (instead of reading from Kafka for now)
            df = self.create_sample_data()
            
            # Show the data
            self.logger.info("Sample data:")
            df.show()
            
            # Write to "MinIO" (local filesystem for testing)
            success = self.write_to_minio(df)
            
            if success:
                self.logger.info("Simple Kafka to MinIO pipeline completed successfully")
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

def main():
    parser = argparse.ArgumentParser(description='Simple Kafka to MinIO Spark Job')
    parser.add_argument('--config', required=True, help='Path to configuration file')
    parser.add_argument('--date', help='Processing date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Validate config file exists
    config_full_path = os.path.join(os.path.dirname(__file__), '..', 'config', args.config)
    if not os.path.exists(config_full_path):
        print(f"Error: Config file not found: {config_full_path}")
        sys.exit(1)
    
    job = SimpleKafkaToMinio(config_full_path, args.date)
    success = job.run()
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
