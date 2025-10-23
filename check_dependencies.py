"""
Simple dependency check for PySpark development
"""

import sys

def check_basic_dependencies():
    """Check only the essential dependencies"""
    essential_packages = [
        "pyspark",
        "minio", 
        "clickhouse_connect",
        "pandas",
        "numpy",
        "pyarrow"
    ]
    
    print("🔍 Checking Essential Dependencies...")
    print("=" * 50)
    
    all_ok = True
    for package in essential_packages:
        try:
            __import__(package)
            print(f"✅ {package}")
        except ImportError as e:
            print(f"❌ {package}: {e}")
            all_ok = False
    
    return all_ok

def test_spark_minimal():
    """Test Spark with absolute minimal configuration"""
    try:
        print("\n🧪 Testing Spark...")
        from pyspark.sql import SparkSession
        
        # Ultra-minimal configuration
        spark = SparkSession.builder \
            .appName("test") \
            .master("local[1]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.sql.legacy.createHiveTableByDefault", "false") \
            .getOrCreate()
            
        version = spark.version
        spark.stop()
        
        print(f"✅ Spark {version} - Session created and stopped")
        return True
        
    except Exception as e:
        print(f"❌ Spark test failed: {e}")
        return False

def main():
    deps_ok = check_basic_dependencies()
    spark_ok = test_spark_minimal()
    
    print("=" * 50)
    
    if deps_ok and spark_ok:
        print("🎉 All essential dependencies working!")
        print("💡 Note: kafka-python removed - using Spark's built-in Kafka connector")
        print("🚀 Ready for PySpark development!")
    else:
        print("💥 Some issues need attention")
    
    return deps_ok and spark_ok

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
