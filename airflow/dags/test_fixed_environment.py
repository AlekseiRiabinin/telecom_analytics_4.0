from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time
import logging

logger = logging.getLogger("airflow.task")

def test_fixed_environment():
    logger.info("🎉 ENVIRONMENT FIXED: Starting execution!")
    
    # Test all the fixed libraries
    import redis
    import celery
    import kombu
    
    logger.info(f"✅ Redis {redis.__version__} - Working!")
    logger.info(f"✅ Celery {celery.__version__} - Working!")
    logger.info(f"✅ Kombu {kombu.__version__} - Working!")
    
    # Test Redis connection
    try:
        r = redis.Redis(host='redis', port=6379)
        logger.info(f"✅ Redis connection: {r.ping()}")
    except Exception as e:
        logger.error(f"❌ Redis connection failed: {e}")
        raise
    
    time.sleep(3)
    logger.info("🚀 ENVIRONMENT FIXED: Task completed successfully!")
    return "Environment fix verified!"

with DAG(
    'test_fixed_environment',
    start_date=datetime(2025, 10, 30),  # Fresh date
    schedule_interval=None,
    catchup=False,
) as dag:

    test_task = PythonOperator(
        task_id='test_fixed_task',
        python_callable=test_fixed_environment
    )
