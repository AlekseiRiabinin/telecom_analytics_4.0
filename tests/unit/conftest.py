import pytest
import tempfile
import os
import configparser
from pathlib import Path
from typing import Generator
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session(test_config_file_path: str) -> Generator:
    """Create a Spark session for testing."""
    
    config = configparser.ConfigParser()
    config.read(test_config_file_path)

    builder: SparkSession.Builder = SparkSession.builder
    
    app_name = config.get('spark', 'app_name', fallback='pytest-spark-test-session')
    master = config.get('spark', 'master', fallback='local[2]')
    builder = builder.appName(app_name).master(master)
    
    if config.has_section('spark'):
        for key, value in config.items('spark'):
            if key not in ['app_name', 'master']:
                spark_key = f"spark.{key.replace('_', '.')}"
                builder = builder.config(spark_key, value)

    if config.has_section('minio'):
        minio_config = config['minio']
        builder = (builder
            .config(
                "spark.hadoop.fs.s3a.endpoint",
                minio_config.get('endpoint', 'http://localhost:9002')
            )
            .config(
                "spark.hadoop.fs.s3a.access.key",
                minio_config.get('access_key', 'test_access')
            )
            .config(
                "spark.hadoop.fs.s3a.secret.key",
                minio_config.get('secret_key', 'test_secret')
            )
            .config(
                "spark.hadoop.fs.s3a.path.style.access",
                "true"
            )
            .config(
                "spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config(
                "spark.hadoop.fs.s3a.connection.ssl.enabled",
                "false"
            )
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
            )
        )    

    spark_session = builder.getOrCreate()
    
    log_level = config.get('spark', 'loglevel', fallback='ERROR')
    spark_session.sparkContext.setLogLevel(log_level)
    
    yield spark_session
    
    spark_session.stop()


@pytest.fixture
def sample_meter_data(spark_session):
    """Create sample meter data for testing"""
    from tests.fixtures.sample_data import create_sample_meter_data
    return create_sample_meter_data(spark_session)


@pytest.fixture
def test_config_parser():
    """Return a config parser with test configuration"""
    config = configparser.ConfigParser()
    
    # Add test configuration sections and values
    config['minio'] = {
        'endpoint': 'http://localhost:9002',
        'access_key': 'test_minioadmin',
        'secret_key': 'test_minioadmin', 
        'bucket': 'test-spark-data'
    }
    
    config['kafka'] = {
        'bootstrap_servers': 'localhost:19092,localhost:19094',
        'topic': 'test_smart_meter_data'
    }
    
    config['mssql'] = {
        'url': 'jdbc:sqlserver://localhost:1433;databaseName=test_telecom_db;trustServerCertificate=true',
        'user': 'sa',
        'password': 'Admin123!',
        'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
    }
    
    config['spark'] = {
        'master': 'local[2]',
        'driver_memory': '1g',
        'executor_memory': '1g',
        'sql.adaptive.enabled': 'false'
    }
    
    config['data_quality'] = {
        'min_voltage': '200',
        'max_voltage': '250',
        'min_current': '0',
        'max_current': '100'
    }
    
    return config


@pytest.fixture
def temp_config_file(test_config_parser):
    """Create a temporary config file for testing using config parser"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False) as f:
        test_config_parser.write(f)
        temp_path = f.name
    
    yield temp_path
    
    # Cleanup
    os.unlink(temp_path)


@pytest.fixture
def test_config_file_path():
    """Get the path to the test config file from fixtures"""
    tests_dir = Path(__file__).parent.parent
    config_path = tests_dir / "fixtures" / "test_configs" / "test_etl_config.conf"
    return str(config_path)


@pytest.fixture
def test_data_dir():
    """Create temporary directory for test data"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir


@pytest.fixture
def mock_minio_config(test_config_parser):
    """Get MinIO configuration for testing"""
    return dict(test_config_parser['minio'])


@pytest.fixture
def mock_kafka_config(test_config_parser):
    """Get Kafka configuration for testing"""
    return dict(test_config_parser['kafka'])


@pytest.fixture
def mock_mssql_config(test_config_parser):
    """Get MSSQL configuration for testing"""
    return dict(test_config_parser['mssql'])
