from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


def create_sample_meter_data(spark: SparkSession) -> DataFrame:
    """Create sample smart meter data for testing."""

    schema = StructType([
        StructField("meter_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("energy_consumption", DoubleType(), True),
        StructField("voltage", DoubleType(), True),
        StructField("current_reading", DoubleType(), True),
        StructField("power_factor", DoubleType(), True),
        StructField("frequency", DoubleType(), True)
    ])
    
    sample_data = [
        ("METER_001", "2024-01-15 10:00:00", 15.75, 220.5, 7.1, 0.95, 50.0),
        ("METER_002", "2024-01-15 10:01:00", 22.30, 219.8, 10.2, 0.92, 49.9),
        ("METER_003", "2024-01-15 10:02:00", 18.45, 221.2, 8.3, 0.98, 50.1),
        ("METER_001", "2024-01-15 11:00:00", 14.20, 220.8, 6.4, 0.96, 50.1),
        ("METER_002", "2024-01-15 11:01:00", 20.15, 219.9, 9.1, 0.93, 49.9),
    ]
    
    return spark.createDataFrame(sample_data, schema)


def create_invalid_meter_data(spark: SparkSession) -> DataFrame:
    """Create data with quality issues for testing."""

    invalid_data = [
        (None, "2024-01-15 10:00:00", 15.75, 220.5, 7.1, 0.95, 50.0),         # null meter_id
        ("METER_002", "2024-01-15 10:01:00", None, 219.8, 10.2, 0.92, 49.9),  # null consumption
        ("METER_003", "2024-01-15 10:02:00", 18.45, 180.0, 8.3, 0.98, 50.1),  # low voltage
        ("METER_004", "2024-01-15 10:03:00", -5.0, 221.2, 8.3, 0.98, 50.1),   # negative consumption
    ]
    
    schema = StructType([
        StructField("meter_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("energy_consumption", DoubleType(), True),
        StructField("voltage", DoubleType(), True),
        StructField("current_reading", DoubleType(), True),
        StructField("power_factor", DoubleType(), True),
        StructField("frequency", DoubleType(), True)
    ])
    
    return spark.createDataFrame(invalid_data, schema)
