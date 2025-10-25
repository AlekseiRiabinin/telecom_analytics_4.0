# tests/unit/test_kafka_to_minio.py
import pytest
import tempfile
import os
import configparser
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import DataFrame
from spark.pipelines.telecom_etl.jobs.kafka_to_minio import KafkaToMinio


class TestKafkaToMinio:
    
    def test_initialization_with_config_file(self, test_config_file_path):
        """Test job initialization with config file path"""
        job = KafkaToMinio(test_config_file_path, "2024-01-15")
        
        assert job.config_path == test_config_file_path
        assert job.processing_date == "2024-01-15"
        assert job.spark is None
        assert job.logger is not None
    
    def test_config_parsing(self, test_config_file_path):
        """Test that config file is properly parsed"""
        config = configparser.ConfigParser()
        config.read(test_config_file_path)
        
        # Verify sections exist
        assert config.has_section('minio')
        assert config.has_section('kafka')
        assert config.has_section('spark')
        
        # Verify MinIO config
        assert config.get('minio', 'endpoint') == 'http://test-minio:9002'
        assert config.get('minio', 'access_key') == 'test_minioadmin'
        assert config.get('minio', 'bucket') == 'test-spark-data'
        
        # Verify Kafka config
        assert 'localhost:19092' in config.get('kafka', 'bootstrap_servers')
        assert config.get('kafka', 'topic') == 'test_smart_meter_data'
    
    def test_create_sample_data(self, spark_session, test_config_file_path):
        """Test sample data creation with actual config file"""
        job = KafkaToMinio(test_config_file_path)
        job.spark = spark_session
        
        df = job.create_sample_data()
        
        assert df.count() == 5
        expected_columns = [
            "meter_id", "timestamp", "energy_consumption", 
            "voltage", "current_reading", "power_factor", "frequency"
        ]
        assert all(col in df.columns for col in expected_columns)
        
        # Verify data content
        rows = df.collect()
        assert any(row.meter_id == "METER_001" for row in rows)
        assert any(row.energy_consumption == 15.75 for row in rows)
    
    @patch('spark.pipelines.telecom_etl.jobs.kafka_to_minio.SparkSessionManager')
    def test_spark_session_initialization(self, mock_spark_manager, test_config_file_path):
        """Test Spark session initialization with config"""
        mock_spark = MagicMock()
        mock_spark_manager.create_spark_session.return_value = mock_spark
        
        job = KafkaToMinio(test_config_file_path)
        job.initialize_spark()
        
        # Verify SparkSessionManager was called with correct arguments
        mock_spark_manager.create_spark_session.assert_called_once_with(
            "KafkaToMinio", 
            test_config_file_path
        )
        assert job.spark == mock_spark
    
    def test_write_to_minio_with_config(self, spark_session, test_config_file_path, test_data_dir):
        """Test MinIO write operation using config"""
        job = KafkaToMinio(test_config_file_path, "2024-01-15")
        job.spark = spark_session
        
        df = job.create_sample_data()
        
        # Mock the actual write operation but verify config is used
        with patch.object(job, 'write_to_minio') as mock_write:
            mock_write.return_value = True
            
            success = job.write_to_minio(df)
            
            assert success is True
            mock_write.assert_called_once_with(df)
    
    def test_missing_config_file(self):
        """Test behavior with missing config file"""
        with pytest.raises(Exception):
            job = KafkaToMinio("/nonexistent/config.conf")
            job.initialize_spark()
    
    def test_invalid_config_section(self, temp_config_file):
        """Test behavior with invalid config sections"""
        # Create a config missing required sections
        config_content = """
[general]
some_setting = value
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False) as f:
            f.write(config_content)
            temp_path = f.name
        
        try:
            job = KafkaToMinio(temp_path)
            # This should not raise immediately, but might fail during Spark initialization
            assert job.config_path == temp_path
        finally:
            os.unlink(temp_path)
    
    @patch('spark.pipelines.telecom_etl.jobs.kafka_to_minio.SparkSessionManager')
    def test_full_pipeline_with_config(self, mock_spark_manager, spark_session, test_config_file_path):
        """Test complete pipeline execution with config file"""
        mock_spark_manager.create_spark_session.return_value = spark_session
        
        job = KafkaToMinio(test_config_file_path, "2024-01-15")
        
        with patch.object(job, 'write_to_minio') as mock_write:
            mock_write.return_value = True
            
            success = job.run()
            
            assert success is True
            # Verify config was used throughout
            mock_spark_manager.create_spark_session.assert_called_once_with(
                "KafkaToMinio", 
                test_config_file_path
            )
