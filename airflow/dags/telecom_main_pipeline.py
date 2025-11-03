import logging
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


logger = logging.getLogger("airflow.task")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


def debug_task():
    logger.info("DEBUG: This task is executing!")
    print("Debug task is working!")
    return "Debug completed"    


def check_spark_connection():
    import socket
    logger.info("Checking Spark connection...")

    try:
        socket.create_connection(('spark-master', 7077), timeout=10)
        logger.info("Spark connection successful!")
        return "Success"

    except Exception as e:
        logger.error(f"Spark connection failed: {e}")
        raise


with DAG(
    'telecom_analytics_etl',
    default_args=default_args,
    description='Telecom Analytics ETL Pipeline',
    schedule_interval=None,
    catchup=False,
    tags=['telecom', 'spark', 'etl'],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id='start_pipeline')
    
    debug = PythonOperator(
        task_id='debug_task',
        python_callable=debug_task
    )
    
    check_conn = PythonOperator(
        task_id='check_spark_connection',
        python_callable=check_spark_connection
    )

    kafka_to_minio = SparkSubmitOperator(
        task_id='kafka_to_minio_job',
        application='/opt/airflow/dags/spark/pipelines/telecom_etl/jobs/kafka_to_minio.py',
        name='telecom-kafka-to-minio',
        conn_id='spark_default',
        application_args=[
            '--config', 'etl_prod.conf', 
            '--date', '{{ ds }}',
            '--prod'
        ],
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,"
                "org.apache.kafka:kafka-clients:3.6.1,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                "com.microsoft.sqlserver:mssql-jdbc:12.4.1.jre11",           
        conf={
            "spark.pyspark.python": "/usr/local/bin/python3.10",
            "spark.pyspark.driver.python": "/usr/local/bin/python3.10",
            "spark.executorEnv.PYSPARK_PYTHON": "/usr/local/bin/python3.10",
            "spark.sql.execution.arrow.pyspark.enabled": "false",
            "spark.network.timeout": "600s",
            "spark.executor.heartbeatInterval": "60s",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9002",
            "spark.hadoop.fs.s3a.access.key": "minioadmin", 
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        },
        driver_memory='2g',
        executor_memory='2g',
        executor_cores=1,
        num_executors=1,
        verbose=True,
        env_vars={
            'PYTHONPATH': '/opt/airflow/dags:/opt/airflow/dags/spark/pipelines',
            'PYSPARK_PYTHON': '/usr/local/bin/python3.10',
            'PYSPARK_DRIVER_PYTHON': '/usr/local/bin/python3.10'
        }
    )

    minio_to_mssql = SparkSubmitOperator(
        task_id='minio_to_mssql_job',
        application='/opt/airflow/dags/spark/pipelines/telecom_etl/jobs/minio_to_mssql.py',
        name='telecom-minio-to-mssql',
        conn_id='spark_default',
        application_args=[
            '--config', 'etl_prod.conf', 
            '--date', '{{ ds }}',
            '--prod'
        ],
        packages="org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                "com.microsoft.sqlserver:mssql-jdbc:12.4.1.jre11",
        conf={
            "spark.pyspark.python": "/usr/local/bin/python3.10",
            "spark.pyspark.driver.python": "/usr/local/bin/python3.10",
            "spark.executorEnv.PYSPARK_PYTHON": "/usr/local/bin/python3.10",
            "spark.sql.execution.arrow.pyspark.enabled": "false",
            "spark.network.timeout": "600s",
            "spark.executor.heartbeatInterval": "60s",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9002",
            "spark.hadoop.fs.s3a.access.key": "minioadmin", 
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        },
        driver_memory='2g',
        executor_memory='2g',
        executor_cores=1,
        num_executors=1,
        verbose=False,
        env_vars={
            'PYTHONPATH': '/opt/airflow/dags:/opt/airflow/dags/spark/pipelines',
            'PYSPARK_PYTHON': '/usr/local/bin/python3.10',
            'PYSPARK_DRIVER_PYTHON': '/usr/local/bin/python3.10'
        }
    )
    
    end = EmptyOperator(task_id='end_pipeline')

    start >> debug >> check_conn >> kafka_to_minio >> minio_to_mssql >> end
