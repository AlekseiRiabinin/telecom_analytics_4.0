"""
Telecom Analytics ETL Pipeline using SparkSubmitOperator
with enhanced configuration and error handling.
"""


from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'telecom',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=2)
}

dag = DAG(
    'telecom_analytics_pipeline',
    default_args=default_args,
    description='Telecom Analytics ETL Pipeline - Spark Jobs Only',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['telecom', 'spark', 'etl', 'batch-processing'],
    max_active_runs=1
)

start_pipeline = EmptyOperator(
    task_id='start_pipeline',
    dag=dag,
)

end_pipeline = EmptyOperator(
    task_id='end_pipeline',
    dag=dag,
)

kafka_to_minio_task = SparkSubmitOperator(
    task_id='kafka_to_minio_job',
    application='/opt/airflow/dags/spark/pipelines/telecom_etl/jobs/kafka_to_minio.py',
    name='telecom-kafka-to-minio',
    conn_id='spark_default',
    application_args=[
        '--config', 'etl_prod.conf',
        '--date', '{{ ds }}'
    ],
    jars=(
        '/opt/airflow/jars/spark-sql-kafka-0-10_2.12-3.5.4.jar,'
        '/opt/airflow/jars/hadoop-aws-3.3.4.jar,'
        '/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,'
        '/opt/airflow/jars/mssql-jdbc-12.4.2.jre11.jar'
    ),
    env_vars={
        'PYTHONPATH': '/opt/airflow/dags:/opt/airflow/dags/spark/pipelines',
        'SPARK_HOME': '/opt/spark'
    },
    conf={
        'spark.master': 'spark://spark-master:7077'
    },
    driver_memory='2g',
    executor_memory='4g',
    executor_cores=2,
    num_executors=2,
    verbose=True,
    dag=dag,
)

minio_to_mssql_task = SparkSubmitOperator(
    task_id='minio_to_mssql_job',
    application='/opt/airflow/dags/spark/pipelines/telecom_etl/jobs/minio_to_mssql.py',
    name='telecom-minio-to-mssql',
    conn_id='spark_default',
    application_args=[
        '--config', 'etl_prod.conf',
        '--date', '{{ ds }}'
    ],
    jars=(
        '/opt/airflow/jars/mssql-jdbc-12.4.2.jre11.jar,'
        '/opt/airflow/jars/hadoop-aws-3.3.4.jar'
    ),
    driver_memory='2g',
    executor_memory='4g',
    executor_cores=2,
    num_executors=2,
    verbose=True,
    dag=dag,
)

start_pipeline >> kafka_to_minio_task >> minio_to_mssql_task >> end_pipeline
