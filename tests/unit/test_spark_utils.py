# tests/unit/test_spark_utils.py
import pytest
import configparser
from unittest.mock import patch, MagicMock
from spark.pipelines.shared.utils.spark_utils import (
    SparkSessionManager, 
    DataQualityChecker,
    SchemaManager
)
from tests.fixtures.sample_data import create_sample_meter_data, create_invalid_meter_data


class TestSparkSessionManager:
    
    def test_create_spark_session_with_config(self, temp_config_file):
        """Test Spark session creation with config file"""
        spark = SparkSessionManager.create_spark_session(
            "test_app", 
            temp_config_file
        )
        
        assert spark is not None
        assert spark.sparkContext.appName == "test_app"
        
        spark.stop()
    
    def test_config_parsing_in_spark_manager(self, test_config_file_path):
        """Test that SparkSessionManager properly parses config"""
        with patch('spark.pipelines.shared.utils.spark_utils.configparser.ConfigParser') as mock_parser:
            mock_config = MagicMock()
            mock_parser.return_value = mock_config
            
            # Mock config sections
            mock_config.__getitem__.return_value = {
                'endpoint': 'http://test:9002',
                'access_key': 'test_key',
                'secret_key': 'test_secret'
            }
            
            spark = SparkSessionManager.create_spark_session("test", test_config_file_path)
            assert spark is not None
            spark.stop()
    
    def test_minio_config_in_spark_session(self, test_config_file_path):
        """Test that MinIO config is applied to Spark session"""
        with patch('pyspark.sql.SparkSession.Builder') as mock_builder:
            mock_instance = MagicMock()
            mock_builder.return_value = mock_instance
            mock_instance.appName.return_value = mock_instance
            mock_instance.config.return_value = mock_instance
            mock_instance.getOrCreate.return_value = MagicMock()
            
            SparkSessionManager.create_spark_session("test", test_config_file_path)
            
            # Verify MinIO config was set
            mock_instance.config.assert_any_call(
                "spark.hadoop.fs.s3a.endpoint", 
                "http://test-minio:9002"
            )
            mock_instance.config.assert_any_call(
                "spark.hadoop.fs.s3a.access.key", 
                "test_minioadmin"
            )
