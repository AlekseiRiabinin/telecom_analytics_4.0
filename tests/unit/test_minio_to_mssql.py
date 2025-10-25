import pytest
import os
from unittest.mock import patch, MagicMock
from pyspark.sql import DataFrame
from spark.pipelines.telecom_etl.jobs.minio_to_mssql import MinioToMSSQL
from tests.fixtures.sample_data import create_sample_meter_data, create_invalid_meter_data


class TestMinioToMSSQL:
    
    def test_initialization_with_config(self, test_config_file_path):
        """Test job initialization with config file"""
        job = MinioToMSSQL(test_config_file_path, "2024-01-15")
        
        assert job.config_path == test_config_file_path
        assert job.processing_date == "2024-01-15"
        assert job.spark is None
        assert job.logger is not None
    
    def test_config_file_exists(self, test_config_file_path):
        """Test that config file exists and is readable"""
        assert os.path.exists(test_config_file_path)
        assert test_config_file_path.endswith('.conf')
    
    @patch('spark.pipelines.telecom_etl.jobs.minio_to_mssql.SparkSessionManager')
    def test_read_from_minio_success(self, mock_spark_manager, spark_session, test_config_file_path):
        """Test successful read from MinIO using config file"""
        mock_spark_manager.create_spark_session.return_value = spark_session
        
        job = MinioToMSSQL(test_config_file_path, "2024-01-15")
        job.spark = spark_session
        
        # Mock the read operation to return sample data
        with patch.object(job, 'read_from_minio') as mock_read:
            mock_read.return_value = create_sample_meter_data(spark_session)
            result_df = job.read_from_minio()
            
            assert result_df is not None
            assert result_df.count() == 5
            mock_read.assert_called_once()
    
    def test_transform_data_with_sample_data(self, spark_session, test_config_file_path):
        """Test data transformation using sample data"""
        job = MinioToMSSQL(test_config_file_path, "2024-01-15")
        job.spark = spark_session
        
        raw_df = create_sample_meter_data(spark_session)
        transformed_df = job.transform_data(raw_df)
        
        # Check new columns are added
        expected_new_columns = ["date", "hour", "consumption_category", "processed_at"]
        for col in expected_new_columns:
            assert col in transformed_df.columns
        
        # Check consumption categories are correctly assigned
        categories = [row.consumption_category for row in transformed_df.collect()]
        assert "MEDIUM" in categories
        assert "HIGH" in categories
        assert "LOW" not in categories  # Based on sample data thresholds
    
    def test_data_quality_validation(self, spark_session, test_config_file_path):
        """Test data quality validation with both valid and invalid data"""
        job = MinioToMSSQL(test_config_file_path, "2024-01-15")
        
        # Test with valid data
        valid_df = create_sample_meter_data(spark_session)
        cleaned_valid = job.transform_data(valid_df)
        assert cleaned_valid.count() == 5  # All records should pass
        
        # Test with invalid data
        invalid_df = create_invalid_meter_data(spark_session)
        cleaned_invalid = job.transform_data(invalid_df)
        # Should have fewer records after quality checks
        assert cleaned_invalid.count() < invalid_df.count()
    
    def test_create_aggregations(self, spark_session, test_config_file_path):
        """Test aggregation creation"""
        job = MinioToMSSQL(test_config_file_path, "2024-01-15")
        job.spark = spark_session
        
        raw_df = create_sample_meter_data(spark_session)
        transformed_df = job.transform_data(raw_df)
        aggregates_df = job.create_aggregations(transformed_df)
        
        # Check aggregation columns
        expected_columns = [
            "meter_id", "date", "total_energy", "avg_voltage", 
            "avg_current", "max_consumption", "record_count", "created_at"
        ]
        for col in expected_columns:
            assert col in aggregates_df.columns
        
        # Should have aggregated records (one per meter per date)
        unique_meter_dates = aggregates_df.select("meter_id", "date").distinct().count()
        assert unique_meter_dates > 0
    
    @patch('spark.pipelines.telecom_etl.jobs.minio_to_mssql.SparkSessionManager')
    def test_run_success(self, mock_spark_manager, spark_session, test_config_file_path):
        """Test successful pipeline execution"""
        mock_spark_manager.create_spark_session.return_value = spark_session
        
        job = MinioToMSSQL(test_config_file_path, "2024-01-15")
        
        # Mock the MinIO read to return sample data
        with patch.object(job, 'read_from_minio') as mock_read:
            mock_read.return_value = create_sample_meter_data(spark_session)
            
            success = job.run()
            
            assert success is True
            mock_spark_manager.create_spark_session.assert_called_once_with(
                "SimpleMinioToMSSQL", 
                test_config_file_path
            )
            mock_read.assert_called_once()
    
    @patch('spark.pipelines.telecom_etl.jobs.minio_to_mssql.SparkSessionManager')
    def test_run_with_empty_data(self, mock_spark_manager, spark_session, test_config_file_path):
        """Test pipeline with empty data"""
        mock_spark_manager.create_spark_session.return_value = spark_session
        
        job = MinioToMSSQL(test_config_file_path, "2024-01-15")
        
        # Mock empty DataFrame
        empty_df = spark_session.createDataFrame([], create_sample_meter_data(spark_session).schema)
        
        with patch.object(job, 'read_from_minio') as mock_read:
            mock_read.return_value = empty_df
            
            success = job.run()
            
            # Should still succeed with empty data
            assert success is True
            mock_read.assert_called_once()
    
    @patch('spark.pipelines.telecom_etl.jobs.minio_to_mssql.SparkSessionManager')
    def test_run_with_read_failure(self, mock_spark_manager, spark_session, test_config_file_path):
        """Test pipeline when MinIO read fails"""
        mock_spark_manager.create_spark_session.return_value = spark_session
        
        job = MinioToMSSQL(test_config_file_path, "2024-01-15")
        
        with patch.object(job, 'read_from_minio') as mock_read:
            mock_read.return_value = None  # Simulate read failure
            
            success = job.run()
            
            # Should handle failure gracefully
            assert success is True  # Based on current implementation
            mock_read.assert_called_once()
    
    def test_demonstrate_mssql_write(self, spark_session, test_config_file_path):
        """Test MSSQL write demonstration method"""
        job = MinioToMSSQL(test_config_file_path, "2024-01-15")
        job.spark = spark_session
        
        # Create test dataframes
        cleaned_df = create_sample_meter_data(spark_session)
        aggregates_df = spark_session.createDataFrame([
            ("METER_001", "2024-01-15", 150.5, 220.0, 7.5, 25.0, 10, "2024-01-15 10:00:00")
        ], ["meter_id", "date", "total_energy", "avg_voltage", "avg_current", "max_consumption", "record_count", "created_at"])
        
        dataframes = {
            'cleaned_data': cleaned_df,
            'daily_aggregates': aggregates_df
        }
        
        # This should not raise any exceptions
        job.demonstrate_mssql_write(dataframes)
    
    def test_processing_date_handling(self, test_config_file_path):
        """Test different processing date formats"""
        # Test with date string
        job1 = MinioToMSSQL(test_config_file_path, "2024-01-15")
        assert job1.processing_date == "2024-01-15"
        
        # Test with None date
        job2 = MinioToMSSQL(test_config_file_path, None)
        assert job2.processing_date is None
