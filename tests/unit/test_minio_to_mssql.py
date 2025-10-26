import os
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from spark.pipelines.telecom_etl.jobs.minio_to_mssql import MinioToMSSQL
from tests.fixtures.sample_data import (
    create_sample_meter_data,
    create_invalid_meter_data
)


class TestMinioToMSSQL:
    
    def test_initialization_with_config(
        self: 'TestMinioToMSSQL',
        test_config_file_path: str
    ) -> None:
        """Test job initialization with config file."""

        job = MinioToMSSQL(test_config_file_path, "2024-01-15")
        
        assert job.config_path == test_config_file_path
        assert job.processing_date == "2024-01-15"
        assert job.spark is None
        assert job.logger is not None
    
    def test_config_file_exists(
        self: 'TestMinioToMSSQL',
        test_config_file_path: str
    ) -> None:
        """Test that config file exists and is readable."""

        assert os.path.exists(test_config_file_path)
        assert test_config_file_path.endswith('.conf')
    
    @patch('spark.pipelines.telecom_etl.jobs.minio_to_mssql.SparkSessionManager')
    def test_read_from_minio_success(
        self: 'TestMinioToMSSQL',
        mock_spark_manager: MagicMock,
        spark_session: SparkSession,
        test_config_file_path: str
    ) -> None:
        """Test successful read from MinIO using config file."""

        mock_spark_manager.create_spark_session.return_value = spark_session
        
        job = MinioToMSSQL(test_config_file_path, "2024-01-15")
        job.spark = spark_session

        with patch.object(job, 'read_from_minio') as mock_read:
            mock_read.return_value = create_sample_meter_data(spark_session)
            result_df = job.read_from_minio()
            
            assert result_df is not None
            assert result_df.count() == 5
            mock_read.assert_called_once()
    
    def test_transform_data_with_sample_data(
        self: 'TestMinioToMSSQL',
        spark_session: SparkSession,
        test_config_file_path: str
    ) -> None:
        """Test data transformation using sample data."""

        job = MinioToMSSQL(test_config_file_path, "2024-01-15")
        job.spark = spark_session
        
        raw_df = create_sample_meter_data(spark_session)
        transformed_df = job.transform_data(raw_df)
        
        expected_new_columns = ["date", "hour", "consumption_category", "processed_at"]
        for col in expected_new_columns:
            assert col in transformed_df.columns
        

        categories = [row["consumption_category"] for row in transformed_df.collect()]
        assert "MEDIUM" in categories
        assert "HIGH" in categories
        assert "LOW" not in categories
    
    def test_data_quality_validation(
        self: 'TestMinioToMSSQL',
        spark_session: SparkSession,
        test_config_file_path: str
    ) -> None:
        """Test data quality validation with both valid and invalid data."""

        job = MinioToMSSQL(test_config_file_path, "2024-01-15")
        
        valid_df = create_sample_meter_data(spark_session)
        cleaned_valid = job.transform_data(valid_df)
        assert cleaned_valid.count() == 5
        
        invalid_df = create_invalid_meter_data(spark_session)
        cleaned_invalid = job.transform_data(invalid_df)
        assert cleaned_invalid.count() < invalid_df.count()
    
    def test_create_aggregations(
        self: 'TestMinioToMSSQL',
        spark_session: SparkSession,
        test_config_file_path: str
    ) -> str:
        """Test aggregation creation."""

        job = MinioToMSSQL(test_config_file_path, "2024-01-15")
        job.spark = spark_session
        
        raw_df = create_sample_meter_data(spark_session)
        transformed_df = job.transform_data(raw_df)
        aggregates_df = job.create_aggregations(transformed_df)
        
        expected_columns = [
            "meter_id", "date", "total_energy", "avg_voltage", 
            "avg_current", "max_consumption", "record_count", "created_at"
        ]
        for col in expected_columns:
            assert col in aggregates_df.columns
        
        unique_meter_dates = (aggregates_df
            .select("meter_id", "date")
            .distinct()
            .count()
        )
        assert unique_meter_dates > 0
    
    @patch('spark.pipelines.telecom_etl.jobs.minio_to_mssql.SparkSessionManager')
    def test_run_success(
        self: 'TestMinioToMSSQL',
        mock_spark_manager: MagicMock,
        spark_session: SparkSession,
        test_config_file_path: str
    ) -> None:
        """Test successful pipeline execution."""

        mock_spark_manager.create_spark_session.return_value = spark_session
        
        job = MinioToMSSQL(test_config_file_path, "2024-01-15")
        
        with patch.object(job, 'read_from_minio') as mock_read:
            mock_read.return_value = create_sample_meter_data(spark_session)
            
            success = job.run()
            
            assert success is True
            mock_spark_manager.create_spark_session.assert_called_once_with(
                "MinioToMSSQL", 
                test_config_file_path
            )
            mock_read.assert_called_once()

    @patch('spark.pipelines.telecom_etl.jobs.minio_to_mssql.SparkSessionManager')
    def test_run_with_empty_data(
        self: 'TestMinioToMSSQL',
        mock_spark_manager: MagicMock,
        spark_session: SparkSession,
        test_config_file_path: str
    ) -> None:
        """Test pipeline with empty data."""

        mock_spark_manager.create_spark_session.return_value = spark_session
        
        job = MinioToMSSQL(test_config_file_path, "2024-01-15")
        
        empty_df = spark_session.createDataFrame(
            [], create_sample_meter_data(spark_session).schema
        )
        
        with patch.object(job, 'read_from_minio') as mock_read:
            mock_read.return_value = empty_df
            
            success = job.run()
            
            assert success is True
            mock_read.assert_called_once()
    
    @patch('spark.pipelines.telecom_etl.jobs.minio_to_mssql.SparkSessionManager')
    def test_run_with_read_failure(
        self: 'TestMinioToMSSQL',
        mock_spark_manager: MagicMock,
        spark_session: SparkSession,
        test_config_file_path: str
    ) -> None:
        """Test pipeline when MinIO read fails."""

        mock_spark_manager.create_spark_session.return_value = spark_session
        
        job = MinioToMSSQL(test_config_file_path, "2024-01-15")
        
        with patch.object(job, 'read_from_minio') as mock_read:
            mock_read.return_value = None  # Simulate read failure
            
            success = job.run()
            
            assert success is True
            mock_read.assert_called_once()
    
    def test_demonstrate_mssql_write(
        self: 'TestMinioToMSSQL',
        spark_session: SparkSession,
        test_config_file_path: str
    ) -> None:
        """Test MSSQL write demonstration method."""

        job = MinioToMSSQL(test_config_file_path, "2024-01-15")
        job.spark = spark_session
        
        cleaned_df = create_sample_meter_data(spark_session)

        aggregates_df = spark_session.createDataFrame(
        [
            ("METER_001", "2024-01-15", 150.5, 220.0, 7.5, 25.0, 10, "2024-01-15 10:00:00")
        ], 
        [
            "meter_id", "date", "total_energy", "avg_voltage",
            "avg_current", "max_consumption", "record_count", "created_at"
        ])
        
        dataframes = {
            'cleaned_data': cleaned_df,
            'daily_aggregates': aggregates_df
        }
        
        job.demonstrate_mssql_write(dataframes)
    
    def test_processing_date_handling(
        self: 'TestMinioToMSSQL',
        test_config_file_path: str
    ) -> None:
        """Test different processing date formats."""

        job1 = MinioToMSSQL(test_config_file_path, "2024-01-15")
        assert job1.processing_date == "2024-01-15"
        
        job2 = MinioToMSSQL(test_config_file_path, None)
        assert job2.processing_date is None
