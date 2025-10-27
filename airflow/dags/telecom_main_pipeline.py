import sys
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta


sys.path.append('/opt/airflow/dags/spark/pipelines')

default_args = {
    'owner': 'telecom',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'telecom_analytics_pipeline',
    default_args=default_args,
    description='Telecom Analytics ETL Pipeline',
    schedule_interval=timedelta(hours=1),
    catchup=False
)

def run_kafka_to_minio(**kwargs):
    """Run Kafka to MinIO job."""
    from telecom_etl.jobs.kafka_to_minio import KafkaToMinio
    
    execution_date = kwargs['execution_date']
    processing_date = execution_date.strftime('%Y-%m-%d')
    
    job = KafkaToMinio(
        config_path="/opt/airflow/dags/spark/pipelines/telecom_etl/config/etl_prod.conf",
        processing_date=processing_date
    )
    success = job.run_production()
    
    if not success:
        raise Exception("Kafka to MinIO job failed")

def run_minio_to_mssql(**kwargs):
    """Run MinIO to MSSQL job."""
    from telecom_etl.jobs.minio_to_mssql import MinioToMSSQL
    
    execution_date = kwargs['execution_date']
    processing_date = execution_date.strftime('%Y-%m-%d')
    
    job = MinioToMSSQL(
        config_path="/opt/airflow/dags/spark/pipelines/telecom_etl/config/etl_prod.conf",
        processing_date=processing_date
    )
    success = job.run_production()
    
    if not success:
        raise Exception("MinIO to MSSQL job failed")

# Define tasks
kafka_to_minio_task = PythonOperator(
    task_id='kafka_to_minio',
    python_callable=run_kafka_to_minio,
    provide_context=True,
    dag=dag,
)

minio_to_mssql_task = PythonOperator(
    task_id='minio_to_mssql',
    python_callable=run_minio_to_mssql,
    provide_context=True,
    dag=dag,
)

# Alternative: Using SparkSubmitOperator
kafka_to_minio_spark_task = SparkSubmitOperator(
    task_id='kafka_to_minio_spark_submit',
    application='/opt/airflow/dags/spark/pipelines/telecom_etl/jobs/kafka_to_minio.py',
    name='kafka_to_minio',
    conn_id='spark_default',
    application_args=[
        '--config', 'etl_prod.conf',
        '--date', '{{ ds }}'
    ],
    jars='/opt/airflow/jars/spark-sql-kafka-0-10_2.12-3.5.4.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar',
    dag=dag,
)

minio_to_mssql_spark_task = SparkSubmitOperator(
    task_id='minio_to_mssql_spark_submit',
    application='/opt/airflow/dags/spark/pipelines/telecom_etl/jobs/minio_to_mssql.py',
    name='minio_to_mssql',
    conn_id='spark_default',
    application_args=[
        '--config', 'etl_prod.conf',
        '--date', '{{ ds }}'
    ],
    jars='/opt/airflow/jars/mssql-jdbc-12.4.2.jre11.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar',
    dag=dag,
)

# Set dependencies
kafka_to_minio_task >> minio_to_mssql_task
# kafka_to_minio_spark_task >> minio_to_mssql_spark_task
