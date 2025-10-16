from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import os
import sys


def test_airflow_environment():
    """Test function to verify the environment"""
    print("🔍 Testing Airflow Environment...")
    print("=" * 50)
    
    # Test Core Configuration
    print("1. Core Configuration:")
    print(f"   DAG Folder: {os.getenv('AIRFLOW__CORE__DAGS_FOLDER', 'Not set')}")
    
    # Test Environment Variables
    print("\n2. Environment Variables:")
    env_vars = [
        'AIRFLOW__CORE__EXECUTOR',
        'AIRFLOW_CONN_HDFS_DEFAULT',
        'AIRFLOW_CONN_KAFKA_DEFAULT', 
        'AIRFLOW_CONN_SPARK_DEFAULT',
        'AIRFLOW_CONN_MINIO_DEFAULT',
        'AIRFLOW_CONN_CLICKHOUSE_DEFAULT'
    ]
    
    for var in env_vars:
        value = os.getenv(var, 'NOT SET')
        masked_value = value[:50] + '...' if len(value) > 50 else value
        print(f"   {var}: {masked_value}")
    
    # Test Python Environment
    print(f"\n3. Python Path: {sys.executable}")
    
    # Test provider imports
    print("\n4. Testing Provider Imports:")
    providers = [
        ('PostgreSQL', 'airflow.providers.postgres'),
        ('Apache HDFS', 'airflow.providers.apache.hdfs'),
        ('Apache Kafka', 'airflow.providers.apache.kafka'),
        ('ClickHouse', 'airflow.providers.clickhouse'),
        ('Apache Spark', 'airflow.providers.apache.spark')
    ]
    
    for provider_name, module_path in providers:
        try:
            __import__(module_path)
            print(f"   ✅ {provider_name}: OK")
        except ImportError as e:
            print(f"   ❌ {provider_name}: MISSING - {e}")
    
    return "Environment test completed!"

def test_connections():
    """Test if connections are accessible"""
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
                print(f"✅ {conn_id}: {conn.conn_type} -> {conn.host}")
            except Exception as e:
                print(f"❌ {conn_id}: ERROR - {e}")
                
    except Exception as e:
        print(f"⚠️ Cannot test connections: {e}")

# Define the DAG
with DAG(
    'telecom_environment_test',
    default_args={
        'owner': 'telecom',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    },
    description='Test Telecom Analytics Environment',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['telecom', 'test'],
) as dag:

    start = EmptyOperator(task_id='start')
    
    test_environment = PythonOperator(
        task_id='test_environment',
        python_callable=test_airflow_environment,
    )
    
    test_connections = PythonOperator(
        task_id='test_connections', 
        python_callable=test_connections,
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> test_environment >> test_connections >> end
