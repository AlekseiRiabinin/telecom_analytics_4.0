from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import logging

logger = logging.getLogger("airflow.task")

def test_kafka_connectivity():
    """Test Kafka broker connectivity"""
    import socket
    logger.info("🔌 Testing Kafka connectivity...")
    
    kafka_hosts = [
        ('kafka-1', 19092),
        ('kafka-2', 19094)
    ]
    
    for host, port in kafka_hosts:
        try:
            socket.create_connection((host, port), timeout=10)
            logger.info(f"✅ Kafka {host}:{port} is reachable")
        except Exception as e:
            logger.error(f"❌ Kafka {host}:{port} failed: {e}")
            raise
    
    return "Kafka connectivity OK"

with DAG(
    'test_kafka_connection',
    start_date=datetime(2025, 10, 28),
    schedule_interval=None,
    catchup=False,
    tags=['test', 'kafka'],
) as dag:

    start = EmptyOperator(task_id='start')
    
    test_kafka = PythonOperator(
        task_id='test_kafka_connectivity',
        python_callable=test_kafka_connectivity
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> test_kafka >> end
