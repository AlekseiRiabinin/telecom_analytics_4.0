#!/usr/bin/env python3
"""
Airflow Connections Test Script
Run this from INSIDE the Airflow container via VS Code Dev Container
"""
import os
import sys
from airflow.models import Variable
from airflow.configuration import conf

def test_airflow_environment():
    print("🔍 Testing Airflow Environment...")
    print("=" * 50)
    
    # Test Core Configuration
    print("1. Core Configuration:")
    print(f"   Executor: {conf.get('core', 'executor')}")
    print(f"   DAG Folder: {conf.get('core', 'dags_folder')}")
    print(f"   Load Examples: {conf.get('core', 'load_examples')}")
    
    # Test Environment Variables
    print("\n2. Environment Variables:")
    env_vars = [
        'AIRFLOW__CORE__EXECUTOR',
        'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN',
        'HADOOP_CONF_DIR',
        'AIRFLOW_CONN_HDFS_DEFAULT',
        'AIRFLOW_CONN_KAFKA_DEFAULT', 
        'AIRFLOW_CONN_SPARK_DEFAULT',
        'AIRFLOW_CONN_MINIO_DEFAULT',
        'AIRFLOW_CONN_CLICKHOUSE_DEFAULT'
    ]
    
    for var in env_vars:
        value = os.getenv(var, 'NOT SET')
        # Mask passwords in connection strings
        if 'password' in var.lower() or '@' in value:
            masked_value = '***MASKED***'
        else:
            masked_value = value[:80] + '...' if len(str(value)) > 80 else value
        print(f"   {var}: {masked_value}")
    
    # Test Python Path and Providers
    print("\n3. Python Environment:")
    print(f"   Python Path: {sys.executable}")
    print(f"   Python Version: {sys.version}")
    
    # Test if providers are accessible
    print("\n4. Testing Provider Imports:")
    providers = [
        ('PostgreSQL', 'airflow.providers.postgres.operators.postgres'),
        ('Apache HDFS', 'airflow.providers.apache.hdfs.sensors.hdfs'),
        ('Apache Kafka', 'airflow.providers.apache.kafka.operators.kafka'),
        ('ClickHouse', 'airflow.providers.clickhouse.operators.clickhouse'),
        ('Apache Spark', 'airflow.providers.apache.spark.operators.spark_submit')
    ]
    
    for provider_name, module_path in providers:
        try:
            __import__(module_path)
            print(f"   ✅ {provider_name}: OK")
        except ImportError as e:
            print(f"   ❌ {provider_name}: MISSING - {e}")
    
    print("\n" + "=" * 50)
    print("✅ Environment check completed!")

def test_connection_objects():
    """Test if we can access connection objects"""
    print("\n5. Testing Connection Objects:")
    try:
        from airflow.hooks.base import BaseHook
        
        connections_to_test = [
            'hdfs_default',
            'kafka_default', 
            'spark_default',
            'minio_default',
            'clickhouse_default'
        ]
        
        for conn_id in connections_to_test:
            try:
                conn = BaseHook.get_connection(conn_id)
                print(f"   ✅ {conn_id}: {conn.conn_type} -> {conn.host}")
            except Exception as e:
                print(f"   ❌ {conn_id}: ERROR - {e}")
                
    except Exception as e:
        print(f"   ⚠️ Cannot test connections: {e}")

if __name__ == "__main__":
    test_airflow_environment()
    test_connection_objects()
