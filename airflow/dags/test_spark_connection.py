from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import logging

logger = logging.getLogger("airflow.task")

def test_spark_connectivity():
    """Test basic Spark master connectivity"""
    import socket
    logger.info("🔌 Testing Spark master connectivity...")
    try:
        socket.create_connection(('spark-master', 7077), timeout=10)
        logger.info("✅ Spark master is reachable on port 7077")
        return "Spark connectivity OK"
    except Exception as e:
        logger.error(f"❌ Spark connection failed: {e}")
        raise

def test_spark_submit():
    """Test if SparkSubmitOperator works"""
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
    logger.info("🔧 Testing SparkSubmitOperator configuration...")
    # Just test if we can import and create the operator
    try:
        spark_op = SparkSubmitOperator(
            task_id='test_spark',
            application='/opt/airflow/dags/test_spark_simple.py',
            name='test-spark-job',
            conn_id='spark_default',
            verbose=True
        )
        logger.info("✅ SparkSubmitOperator configured successfully")
        return "SparkSubmitOperator OK"
    except Exception as e:
        logger.error(f"❌ SparkSubmitOperator failed: {e}")
        raise

with DAG(
    'test_spark_connection',
    start_date=datetime(2025, 10, 28),
    schedule_interval=None,
    catchup=False,
    tags=['test', 'spark'],
) as dag:

    start = EmptyOperator(task_id='start')
    
    test_connectivity = PythonOperator(
        task_id='test_spark_connectivity',
        python_callable=test_spark_connectivity
    )
    
    test_operator = PythonOperator(
        task_id='test_spark_operator',
        python_callable=test_spark_submit
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> test_connectivity >> test_operator >> end
