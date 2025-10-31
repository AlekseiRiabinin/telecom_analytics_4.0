"""
Utilities: SparkSessionManager, SchemaManager and DataQualityChecker.   
"""


import configparser
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col
from pyspark.sql import DataFrame


logger = logging.getLogger(__name__)


class SparkSessionManager:
    
    @staticmethod
    def create_spark_session(app_name: str, config_path: str) -> SparkSession:
        """Create and configure a Spark session."""

        config = configparser.ConfigParser()
        config.read(config_path)
        
        # Define with explicit type hint
        builder: SparkSession.Builder = SparkSession.builder
        builder = builder.appName(app_name)

        spark_config = config['spark']
        master_url = spark_config.get('master', 'local[*]')
        builder = builder.master(master_url)

        # Common configurations
        builder = (builder
            .config("spark.pyspark.python", "/usr/bin/python3")
            .config("spark.pyspark.driver.python", "/usr/bin/python3")
            .config("spark.executorEnv.PYSPARK_PYTHON", "/usr/bin/python3")
            .config("spark.sql.execution.arrow.pyspark.enabled", "false")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.network.timeout", "600s")
            .config("spark.executor.heartbeatInterval", "60s")
        )

        # MinIO configurations
        minio_config = config['minio']
        builder = (builder
            .config("spark.hadoop.fs.s3a.endpoint", minio_config['endpoint'])
            .config("spark.hadoop.fs.s3a.access.key", minio_config['access_key'])
            .config("spark.hadoop.fs.s3a.secret.key", minio_config['secret_key'])
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        )
        
        spark_session: SparkSession = builder.getOrCreate()
        return spark_session

class SchemaManager:
    @staticmethod
    def get_smart_meter_schema() -> StructType:
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
    def validate_meter_data(df: DataFrame) -> DataFrame:       
        initial_count = df.count()
        cleaned_df = df.filter(
            col("meter_id").isNotNull() &
            col("energy_consumption").isNotNull() &
            col("voltage").between(200, 250) &
            col("current_reading").between(0, 100) &
            (col("energy_consumption") > 0)
        )
        cleaned_count = cleaned_df.count()
        logging.info(
            f"Data quality: {cleaned_count}/{initial_count} "
            f"records passed validation"
        )
        return cleaned_df
