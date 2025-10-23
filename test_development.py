#!/usr/bin/env python3
"""
Test that you can develop PySpark jobs locally
"""

def test_spark_imports():
    """Test that you can import Spark modules (for development)"""
    print("🧪 Testing PySpark imports for development...")
    
    try:
        # These imports should work for code completion
        from pyspark.sql import SparkSession, DataFrame
        from pyspark.sql.functions import col, lit, when
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType
        
        print("✅ All PySpark imports successful")
        print("💡 You can now write Spark code with full IDE support")
        return True
        
    except Exception as e:
        print(f"❌ PySpark imports failed: {e}")
        return False

def test_business_logic():
    """Test the data transformation logic without Spark"""
    print("\n🧪 Testing business logic...")
    
    # Sample data transformation logic
    sample_record = {
        "meter_id": "METER_001",
        "timestamp": "2024-01-15 10:00:00", 
        "energy_consumption": 15.75,
        "voltage": 220.5,
        "current_reading": 7.1,
        "power_factor": 0.95,
        "frequency": 50.0
    }
    
    # Data validation (same logic as in your Spark job)
    is_valid = (
        sample_record["energy_consumption"] > 0 and
        sample_record["voltage"] >= 200 and 
        sample_record["voltage"] <= 250 and
        sample_record["current_reading"] >= 0 and
        sample_record["current_reading"] <= 100
    )
    
    # Categorization logic
    consumption = sample_record["energy_consumption"]
    if consumption < 10:
        category = "LOW"
    elif consumption < 25:
        category = "MEDIUM" 
    else:
        category = "HIGH"
    
    print(f"✅ Data validation: {'PASS' if is_valid else 'FAIL'}")
    print(f"✅ Consumption category: {category}")
    
    return is_valid

def main():
    print("🔧 Testing PySpark Development Setup")
    print("=" * 50)
    
    imports_ok = test_spark_imports()
    logic_ok = test_business_logic()
    
    print("=" * 50)
    
    if imports_ok and logic_ok:
        print("🎉 Development setup verified!")
        print("💡 You can:")
        print("   • Write Spark code with autocomplete")
        print("   • Test business logic locally") 
        print("   • Submit jobs to Docker Spark via Airflow")
        print("\n🚀 Ready to develop your kafka_to_minio.py and minio_to_mssql.py jobs!")
    else:
        print("💥 Some issues need attention")
    
    return imports_ok and logic_ok

if __name__ == "__main__":
    success = main()
