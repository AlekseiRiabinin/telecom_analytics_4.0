from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import configparser
import logging


class SparkSessionManager:
    @staticmethod
    def create_spark_session(app_name, config_path):
        config = configparser.ConfigParser()
        config.read(config_path)
        
        spark_builder = SparkSession.builder.appName(app_name)
        
        # Common configurations
        spark_builder.config("spark.sql.adaptive.enabled", "true")
        spark_builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        # MinIO configurations
        spark_builder.config("spark.hadoop.fs.s3a.endpoint", config['minio']['endpoint'])
        spark_builder.config("spark.hadoop.fs.s3a.access.key", config['minio']['access_key'])
        spark_builder.config("spark.hadoop.fs.s3a.secret.key", config['minio']['secret_key'])
        spark_builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
        spark_builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark_builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        
        return spark_builder.getOrCreate()

class SchemaManager:
    @staticmethod
    def get_smart_meter_schema():
        return StructType([
            StructField("meter_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("energy_consumption", DoubleType(), True),
            StructField("voltage", DoubleType(), True),
            StructField("current_reading", DoubleType(), True),
            StructField("power_factor", DoubleType(), True),
            StructField("frequency", DoubleType(), True)
        ])

class DataQualityChecker:
    @staticmethod
    def validate_meter_data(df):
        from pyspark.sql.functions import col
        initial_count = df.count()
        cleaned_df = df.filter(
            col("meter_id").isNotNull() &
            col("energy_consumption").isNotNull() &
            col("energy_consumption") > 0 &
            col("voltage").between(200, 250) &
            col("current_reading").between(0, 100)
        )
        cleaned_count = cleaned_df.count()
        logging.info(f"Data quality: {cleaned_count}/{initial_count} records passed validation")
        return cleaned_df
