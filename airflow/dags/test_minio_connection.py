from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import logging

logger = logging.getLogger("airflow.task")

def test_minio_connectivity():
    """Test MinIO connectivity"""
    try:
        from minio import Minio
        logger.info("🔌 Testing MinIO connectivity...")
        
        # Create MinIO client
        client = Minio(
            "minio:9002",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )
        
        # List buckets to test connection
        buckets = client.list_buckets()
        bucket_names = [bucket.name for bucket in buckets]
        
        logger.info(f"✅ MinIO connection successful. Buckets: {bucket_names}")
        return "MinIO connectivity OK"
        
    except Exception as e:
        logger.error(f"❌ MinIO connection failed: {e}")
        raise

with DAG(
    'test_minio_connection',
    start_date=datetime(2025, 10, 28),
    schedule_interval=None,
    catchup=False,
    tags=['test', 'minio'],
) as dag:

    start = EmptyOperator(task_id='start')
    
    test_minio = PythonOperator(
        task_id='test_minio_connectivity',
        python_callable=test_minio_connectivity
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> test_minio >> end
