"""
Simple Spark job for testing
"""

from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("SimpleSparkTest") \
        .getOrCreate()
    
    # Create simple data
    data = [("Test", 1), ("Spark", 2), ("Job", 3)]
    df = spark.createDataFrame(data, ["Word", "Count"])
    
    print("✅ Spark session created successfully")
    print(f"✅ DataFrame count: {df.count()}")
    print(f"✅ Spark version: {spark.version}")
    
    df.show()
    spark.stop()

if __name__ == "__main__":
    main()
