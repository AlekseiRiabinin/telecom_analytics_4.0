"""
Check if environment is ready for PySpark development
Note: Spark jobs will run in Docker, not locally
"""

import sys

def check_dependencies():
    """Check libraries needed for PySpark development"""
    packages = {
        "pyspark": "Spark library (for code completion)",
        "minio": "MinIO/S3 client", 
        "clickhouse_connect": "ClickHouse client",
        "pandas": "Data manipulation",
        "numpy": "Numerical computing",
        "pyarrow": "Apache Arrow support",
        "python_dotenv": "Environment variables",
        "configparser": "Configuration files",
    }
    
    print("🔍 Checking PySpark Development Environment")
    print("=" * 60)
    print("💡 Note: Spark jobs will execute in Docker cluster")
    print("=" * 60)
    
    all_ok = True
    for package, description in packages.items():
        try:
            if package == "python_dotenv":
                import dotenv as _
                display_name = "python-dotenv"
            else:
                __import__(package)
                display_name = package
            print(f"✅ {display_name:20} - {description}")
        except ImportError as e:
            print(f"❌ {package:20} - {description}")
            print(f"   Error: {e}")
            all_ok = False
    
    print("=" * 60)
    
    # Check Java version (informational)
    import subprocess
    try:
        result = subprocess.run(['java', '-version'], capture_output=True, text=True)
        java_version = result.stderr.split('\n')[0] if result.stderr else "Unknown"
        print(f"ℹ️  Java: {java_version}")
        print("   ⚠️  Note: Java 21 may cause local Spark issues")
        print("   💡 Spark jobs will run in Docker (Java 11/17)")
    except:
        print("ℹ️  Java: Not found (OK - using Docker Spark)")
    
    if all_ok:
        print("\n🎉 Development environment ready!")
        print("🚀 You can now:")
        print("   • Write PySpark jobs")
        print("   • Test business logic locally") 
        print("   • Submit jobs via Airflow to Docker Spark cluster")
    else:
        print("\n💥 Some dependencies missing")
    
    return all_ok

if __name__ == "__main__":
    success = check_dependencies()
    sys.exit(0 if success else 1)
