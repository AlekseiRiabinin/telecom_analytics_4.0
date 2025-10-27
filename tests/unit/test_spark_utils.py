from spark.pipelines.shared.utils.spark_utils import SparkSessionManager


class TestSparkSessionManager:
    
    def test_create_spark_session_with_config(
        self: 'TestSparkSessionManager',
        temp_config_file: str
    ) -> None:
        """Test Spark session creation with config file."""

        spark = SparkSessionManager.create_spark_session(
            "test_app", 
            temp_config_file
        )
        
        assert spark is not None
        assert spark.sparkContext.appName == "test_app"
        
        spark.stop()
    
    def test_config_parsing_in_spark_manager(
        self: 'TestSparkSessionManager',
        test_config_file_path: str
    ) -> None:
        """Test that SparkSessionManager properly parses config."""

        spark = SparkSessionManager.create_spark_session(
            "test", test_config_file_path
        )
        
        try:
            assert spark is not None
            assert spark.conf.get("spark.hadoop.fs.s3a.endpoint") is not None

        finally:
            spark.stop()
    
    def test_minio_config_in_spark_session(
        self: 'TestSparkSessionManager',
        test_config_file_path: str
    ) -> None:
        """Test that MinIO config is applied to Spark session."""

        spark = SparkSessionManager.create_spark_session(
            "test", test_config_file_path
        )

        try:
            endpoint = spark.conf.get("spark.hadoop.fs.s3a.endpoint")
            access_key = spark.conf.get("spark.hadoop.fs.s3a.access.key")
            
            assert endpoint == "http://localhost:9002"
            assert access_key == "test_minioadmin"
            assert spark.conf.get("spark.hadoop.fs.s3a.path.style.access") == "true"
            assert spark.conf.get("spark.hadoop.fs.s3a.connection.ssl.enabled") == "false"
            
        finally:
            spark.stop()
