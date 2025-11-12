"""
CSV MinIO to MSSQL Spark Job
Reads CSV from MinIO and loads into MSSQL SalesData table.
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
from pyspark.sql.types import *
from pyspark.sql.functions import *
from spark.pipelines.shared.utils.spark_utils import SparkSessionManager


class Arguments(TypedDict):
    config: str
    date: str | None
    prod: bool


class CsvMinioToMSSQL:
    """Read CSV from MinIO and load into MSSQL SalesData table."""
    
    def __init__(
        self: 'CsvMinioToMSSQL',
        config_path: str,
        processing_date: datetime = None
    ) -> None:
        self.config_path = config_path
        self.processing_date = processing_date
        self.spark = None
        self.logger = self.setup_logging()

    def setup_logging(self: 'CsvMinioToMSSQL') -> logging:
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger('CsvMinioToMSSQL')
    
    def initialize_spark(self: 'CsvMinioToMSSQL') -> SparkSession:
        """Initialize Spark session."""

        self.logger.info("Initializing Spark session")
        self.spark = SparkSessionManager.create_spark_session(
            "MinioToMSSQL", 
            self.config_path
        )

    def define_sales_data_schema(self: 'CsvMinioToMSSQL') -> StructType:
        """Define the schema for SalesData table matching MSSQL structure."""

        return StructType([
            StructField("КодПериод", IntegerType(), False),
            StructField("КодКлиентВх", IntegerType(), False),
            StructField("КодАдресВх", IntegerType(), False),
            StructField("КодГруппаОтчетности", IntegerType(), False),
            StructField("КодПодгруппаОтчетности", IntegerType(), False),
            StructField("КодНоменклатураВх", IntegerType(), False),
            StructField("КодЦенаПродажи", IntegerType(), False),
            StructField("КодВходнаяЦена", IntegerType(), False),
            StructField("Продажи, шт.", FloatType(), True),
            StructField("Продажи в руб.", FloatType(), True),
            StructField("Себестоимость в руб.", FloatType(), True),
            StructField("КодНаименованиеФайла", LongType(), False),
            StructField("КодПоставщикРЦ", IntegerType(), False),
            StructField("КодПроизводитель", IntegerType(), False),
            StructField("Дата", DateType(), False),
            StructField("Количество ТТ", IntegerType(), True),
            StructField("КодЕдиницаИзмерения", IntegerType(), False),
            StructField("КодСтатусОтчета", IntegerType(), False),
            StructField("КодНаименованиеФайлаВнутренний", LongType(), True),
            StructField("ПринадлежностьКод", IntegerType(), False),
            StructField("КластерЗаливки", IntegerType(), False),
            StructField("pred_key", StringType(), True)
        ])

    def read_csv_from_minio_prod(self: 'CsvMinioToMSSQL') -> DataFrame:
        """Read CSV data from MinIO in production."""

        try:
            config = configparser.ConfigParser()
            config.read(self.config_path)
            minio_config = config['minio']
            
            input_path = f"s3a://{minio_config['raw_bucket']}/data/sales_data_sample.csv"
            
            self.logger.info(f"Reading CSV from MinIO: {input_path}")
            
            schema = self.define_sales_data_schema()
            
            df = (self.spark.read
                .option("header", "true")
                .option("delimiter", ";")
                .option("encoding", "UTF-8")
                .option("inferSchema", "false")
                .schema(schema)
                .csv(input_path)
            )
            
            record_count = df.count()
            self.logger.info(f"Successfully read {record_count} records from CSV")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read CSV from MinIO: {str(e)}")
            return None

    def write_to_mssql_prod(self: 'CsvMinioToMSSQL', df: DataFrame) -> bool:
        """Write data to MSSQL SalesData table in production."""
        
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

            self.logger.info(f"Writing {df.count()} records to SalesData table")
            
            (df.write
                .format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", "SalesData")
                .option("user", properties["user"])
                .option("password", properties["password"])
                .option("driver", properties["driver"])
                .mode("append")
                .save()
            )

            self.logger.info("Successfully wrote data to SalesData table")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to write to MSSQL: {str(e)}")
            import traceback
            self.logger.error(f"Detailed error: {traceback.format_exc()}")
            return False

    def run_prod(self: 'CsvMinioToMSSQL') -> bool:
        """Execute the production pipeline."""

        try:
            self.initialize_spark()

            raw_df = self.read_csv_from_minio_prod()
            
            if raw_df is None or raw_df.count() == 0:
                self.logger.warning("No CSV data found to process")
                return True

            self.logger.info("Sample of raw CSV data:")
            raw_df.show(5)
            
            success = self.write_to_mssql_prod(raw_df)
            
            if success:
                self.logger.info("CSV MinIO to MSSQL pipeline completed successfully")
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


def main() -> None:
    parser = argparse.ArgumentParser(description='CSV MinIO to MSSQL Spark Job')
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

    job = CsvMinioToMSSQL(config_full_path, typed_args['date'])

    if typed_args['prod']:
        print("Running in PRODUCTION mode (reading CSV from MinIO, writing to MSSQL)")
        success = job.run_prod()
    else:
        print("Development mode not implemented for CSV pipeline")
        success = False
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
