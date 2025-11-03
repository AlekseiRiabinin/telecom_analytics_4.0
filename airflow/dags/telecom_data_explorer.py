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
    logger.info("DEBUG: Data Explorer task is executing!")
    print("Data Explorer debug task is working!")
    return "Data Explorer debug completed"    


def check_spark_connection():
    import socket
    logger.info("Checking Spark connection for Data Explorer...")

    try:
        socket.create_connection(('spark-master', 7077), timeout=10)
        logger.info("Spark connection successful for Data Explorer!")
        return "Success"

    except Exception as e:
        logger.error(f"Spark connection failed for Data Explorer: {e}")
        raise


def exploration_complete():
    logger.info("Data exploration completed! Check the logs for comprehensive data analysis.")
    logger.info("The exploration includes:")
    logger.info("  - Basic data statistics and schema information")
    logger.info("  - NULL value analysis for all columns") 
    logger.info("  - Data quality validation against ETL rules")
    logger.info("  - Detailed failure analysis for invalid records")
    logger.info("  - Summary report with data quality scoring")
    return "Data exploration completed successfully"


with DAG(
    'telecom_data_explorer',
    default_args=default_args,
    description='Data exploration and quality analysis for MinIO data',
    schedule_interval=None,
    catchup=False,
    tags=['telecom', 'debug', 'data-quality', 'exploration'],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id='start_data_exploration')
    
    debug = PythonOperator(
        task_id='debug_exploration_task',
        python_callable=debug_task
    )
    
    check_conn = PythonOperator(
        task_id='check_spark_connection_exploration',
        python_callable=check_spark_connection
    )

    explore_minio_data = SparkSubmitOperator(
        task_id='explore_minio_data',
        application='/opt/airflow/dags/spark/pipelines/telecom_etl/jobs/minio_data_explorer.py',
        name='telecom-minio-data-explorer',
        conn_id='spark_default',
        application_args=[
            '--config', 'etl_prod.conf', 
            '--date', '{{ ds }}',
            '--prod'
        ],
        packages="org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
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
    
    completion_notice = PythonOperator(
        task_id='exploration_complete',
        python_callable=exploration_complete
    )

    end = EmptyOperator(task_id='end_data_exploration')

    start >> debug >> check_conn >> explore_minio_data >> completion_notice >> end
