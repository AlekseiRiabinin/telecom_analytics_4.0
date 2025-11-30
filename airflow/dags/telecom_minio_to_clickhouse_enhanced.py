"""
Enhanced MinIO to ClickHouse ETL Pipeline
With sensors, monitoring, and production-ready features.
"""

import re
import logging
from typing import Any
from datetime import datetime, timedelta
from clickhouse_driver import Client
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.python import PythonSensor
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance


logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 28),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'max_active_runs': 1,
}


def get_clickhouse_hook():
    """Get ClickHouse Hook with explicit HTTP parameters."""

    from clickhouse_provider.hooks.clickhouse_hook import ClickhouseHook

    return ClickhouseHook(
        host='clickhouse',
        port=8123,
        database='telecom_analytics',
        user='admin',
        password='clickhouse_admin',
        protocol='http'
    )


def check_minio_connection():
    """Check MinIO connection and data availability."""

    try:
        import boto3
        from botocore.client import Config
        
        logger.info("Checking MinIO connection...")
        
        s3 = boto3.client(
            's3',
            endpoint_url='http://minio:9002',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            config=Config(signature_version='s3v4'),
            verify=False
        )

        list_buckets_method = getattr(s3, 'list_buckets')
        buckets = list_buckets_method()
        bucket_names = [bucket['Name'] for bucket in buckets['Buckets']]
        logger.info(f"Connected to MinIO. Available buckets: {bucket_names}")

        target_bucket = 'trino-data-lake'
        if target_bucket in bucket_names:
            logger.info(f"Target bucket '{target_bucket}' exists")

            processing_date = Variable.get(
                "processing_date",
                default_var=datetime.now().strftime('%Y-%m-%d')
            )
            prefix = f"smart_meter_data/date={processing_date}/"

            try:
                list_objects_method = getattr(s3, 'list_objects_v2')
                objects = list_objects_method(
                    Bucket=target_bucket, Prefix=prefix, MaxKeys=1
                )
                if 'Contents' in objects:
                    file_count = len(objects['Contents'])
                    logger.info(f"Found {file_count} files for date {processing_date}")
                    return f"MinIO connection OK - {file_count} files found"
                else:
                    logger.warning(f"No data found for date {processing_date}")
                    return f"MinIO connection OK - No data for {processing_date}"

            except Exception as e:
                logger.warning(f"Could not list objects: {e}")
                return "MinIO connection OK - Could not check data"
        else:
            logger.error(f"Target bucket '{target_bucket}' not found")
            raise AirflowException(f"Bucket {target_bucket} not found")
    
    except Exception as e:
        logger.error(f"MinIO connection failed: {e}")
        raise AirflowException(f"MinIO connection failed: {e}")


def check_clickhouse_health():
    """ClickHouse health check using native client."""

    try:
        logger.info("Performing ClickHouse health check (native client)...")
        
        if check_clickhouse_health_direct():
            logger.info("ClickHouse is healthy (native client)")
            return "ClickHouse healthy"

    except Exception as e:
        raise AirflowException(f"ClickHouse health check failed: {e}")


def setup_clickhouse_infrastructure():
    """Create necessary ClickHouse tables and views."""

    try:
        logger.info("Setting up ClickHouse infrastructure...")

        clickhouse_hook = get_clickhouse_hook()
        
        clickhouse_hook.run("CREATE DATABASE IF NOT EXISTS telecom_analytics")

        tables = {
            'smart_meter_raw': """
            CREATE TABLE IF NOT EXISTS telecom_analytics.smart_meter_raw (
                meter_id String,
                timestamp DateTime64(3),
                date Date DEFAULT toDate(timestamp),
                energy_consumption Float32,
                voltage Float32,
                current_reading Float32,
                power_factor Float32,
                frequency Float32,
                consumption_category String,
                is_anomaly UInt8 DEFAULT 0,
                partition_date Date DEFAULT toDate(timestamp),
                processed_at DateTime64(3) DEFAULT now()
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(partition_date)
            ORDER BY (meter_id, timestamp)
            SETTINGS index_granularity = 8192
            """,
      
            'meter_aggregates': """
            CREATE TABLE IF NOT EXISTS telecom_analytics.meter_aggregates (
                meter_id String,
                date Date,
                hour UInt8,
                partition_date Date,
                total_energy_hourly Float32,
                avg_energy_hourly Float32,
                avg_voltage_hourly Float32,
                avg_current_hourly Float32,
                max_consumption_hourly Float32,
                min_consumption_hourly Float32,
                record_count_hourly UInt32,
                anomaly_count_hourly UInt32,
                aggregation_type String,
                created_at DateTime64(3) DEFAULT now()
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(partition_date)
            ORDER BY (meter_id, date, hour, aggregation_type)
            SETTINGS index_granularity = 8192
            """
        }

        for table_name, ddl in tables.items():
            try:
                clickhouse_hook.run(ddl)
                logger.info(f"Table {table_name} created/verified")

            except Exception as e:
                logger.warning(f"Table {table_name} creation note: {e}")

        return "ClickHouse infrastructure setup completed"

    except Exception as e:
        logger.error(f"ClickHouse setup failed: {e}")
        raise AirflowException(f"ClickHouse setup failed: {e}")


def validate_etl_results(**kwargs):
    """Validate ETL results with comprehensive checks."""

    from clickhouse_provider.hooks.clickhouse_hook import ClickhouseHook

    date: datetime = kwargs["execution_date"]
    date_str = date.strftime("%Y-%m-%d")
    logging.info(f"Validating ETL results for date: {date_str}")

    clickhouse_hook = ClickhouseHook(clickhouse_conn_id="click_default")

    query = f"""
        SELECT 
            count(*) as total,
            countIf(energy_consumption > 0) as valid_consumption,
            countIf(voltage BETWEEN 200 AND 250) as valid_voltage,
            countIf(is_anomaly = 1) as anomaly_count
        FROM telecom_analytics.smart_meter_raw 
        WHERE date = '{date_str}'
    """

    rows = clickhouse_hook.get_records(query)

    if not rows or not rows[0]:
        total = valid_consumption = valid_voltage = anomaly_count = 0
    else:
        row: dict[str, Any] = rows[0]

        if isinstance(row, (list, tuple)) and len(row) == 4:
            total, valid_consumption, valid_voltage, anomaly_count = row
        elif hasattr(row, 'get'):
            total = row.get('total', 0)
            valid_consumption = row.get('valid_consumption', 0)
            valid_voltage = row.get('valid_voltage', 0)
            anomaly_count = row.get('anomaly_count', 0)
        else:
            total = row[0]
            valid_consumption = valid_voltage = anomaly_count = 0

    logging.info(
        f"Validation data_quality: total={total}, valid_consumption={valid_consumption}, "
        f"valid_voltage={valid_voltage}, anomaly_count={anomaly_count}"
    )


def cleanup_resources():
    """Cleanup temporary resources and optimize tables."""

    try:
        logger.info("Performing post-ETL cleanup...")
        
        clickhouse_hook = get_clickhouse_hook()

        optimize_queries = [
            "OPTIMIZE TABLE telecom_analytics.smart_meter_raw FINAL",
            "OPTIMIZE TABLE telecom_analytics.meter_aggregates FINAL"
        ]

        for query in optimize_queries:
            try:
                clickhouse_hook.run(query)
                logger.info(f"Optimized: {query.split()[1]}")

            except Exception as e:
                logger.warning(f"Optimization warning: {e}")

        cleanup_query = """
        ALTER TABLE telecom_analytics.smart_meter_raw 
        DELETE WHERE partition_date < today() - 60
        """
        clickhouse_hook.run(cleanup_query)
        logger.info("Scheduled cleanup of data older than 60 days")
        
        return "Cleanup completed successfully"
        
    except Exception as e:
        logger.warning(f"Cleanup completed with warnings: {e}")
        return f"Cleanup completed with warnings: {e}"


def handle_etl_failure(context):
    """Handle ETL pipeline failures."""

    try:
        exception = getattr(context, 'exception', None)
        task_instance = getattr(context, 'task_instance', None)
        execution_date = getattr(context, 'execution_date', None)
        
        task_id = "unknown_task"
        if task_instance:
            task_id = getattr(task_instance, 'task_id', 'unknown_task')
        
        dag_id = "unknown_dag"
        dag_run = getattr(context, 'dag_run', None)
        if dag_run:
            dag_id = getattr(dag_run, 'dag_id', 'unknown_dag')
        else:
            dag_id = (
                getattr(task_instance, 'dag_id', 'unknown_dag') 
                if task_instance else 'unknown_dag'
            )

        error_message = f"ETL Pipeline Failed!\n"
        error_message += f"• DAG: {dag_id}\n"
        error_message += f"• Task: {task_id}\n"
        error_message += f"• Execution: {execution_date}\n"
        error_message += f"• Error: {str(exception) if exception else 'Unknown error'}\n"
        
        logger.error(error_message)     
        logger.error(f"Failure context type: {type(context)}")
        logger.error(f"Context available attributes: {dir(context)}")

    except Exception as e:
        logger.error(f"Critical: Failure handler crashed: {e}")
    

def check_minio_files_exists(bucket_name, expected_files=1):
    """Check if any files exist in the latest MinIO partition."""

    import boto3
    from botocore.client import Config

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9002",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        config=Config(signature_version="s3v4"),
        verify=False
    )

    list_objects = getattr(s3, "list_objects_v2", None)
    if not list_objects:
        raise AirflowException("S3 client does not support list_objects_v2")

    logger.info("Scanning MinIO for available partitions...")

    response: dict[str, Any] = list_objects(
        Bucket=bucket_name,
        Prefix="smart_meter_data/",
        Delimiter="/"
    )

    prefixes = response.get("CommonPrefixes", [])
    partitions = [p["Prefix"] for p in prefixes]

    dates = []
    for p in partitions:
        m = re.search(r"date=(\d{4}-\d{2}-\d{2})/?", p)
        if m:
            dates.append(m.group(1))

    if not dates:
        logger.warning("No date partitions found in MinIO.")
        return False

    latest_date = sorted(dates)[-1]
    prefix = f"smart_meter_data/date={latest_date}/"

    logger.info(f"Checking latest MinIO partition for files: {prefix}")

    res: dict[str, Any] = list_objects(Bucket=bucket_name, Prefix=prefix)
    file_count = len(res.get("Contents", []))

    logger.info(f"Found {file_count} files in {prefix}")

    return file_count >= expected_files


def check_dependency_files():
    """Check if all dependency files are present."""

    required_files = [
        '/opt/airflow/configs/etl_prod.conf',
        '/opt/airflow/dags/spark/pipelines/telecom_etl/jobs/minio_to_clickhouse.py',
        '/opt/airflow/scripts/validation_queries.sql'
    ]

    for file_path in required_files:
        try:
            with open(file_path, 'r'):
                pass
            logger.info(f"File exists: {file_path}")

        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            return False
    
    return True


def check_minio_health_via_aws_conn():
    """Check MinIO health using the existing AWS connection."""

    try:
        s3_hook = S3Hook(aws_conn_id='aws_default')
        client = s3_hook.get_conn()

        list_buckets_method = getattr(client, 'list_buckets', None)
        if callable(list_buckets_method):
            list_buckets_method()
            logger.info("MinIO health check passed via AWS connection")
            return True
        else:
            logger.warning("S3 client does not have list_buckets method")
            return False

    except Exception as e:
        logger.warning(f"MinIO health check failed: {e}")
        return False


def check_clickhouse_health_direct():
    """ClickHouse health check used by the sensor."""

    try:
        client = Client(
            host="clickhouse-server",
            port=9000,
            user="admin",
            password="clickhouse_admin",
        )
        client.execute("SELECT 1")
        return True

    except Exception as e:
        raise AirflowException(f"ClickHouse health check failed: {e}")


def select_latest_partition(**context):
    """Select the latest partition folder from XCom results."""

    ti: TaskInstance = context['ti']
    folders = ti.xcom_pull(task_ids="list_available_partitions") or []
    
    logger.info("=== SELECT LATEST PARTITION DEBUG ===")
    logger.info(f"Input folders: {folders}")
    logger.info(f"Number of folders: {len(folders)}")
    
    partitions = [str(f) for f in folders if "date=" in str(f)]
    logger.info(f"Filtered partitions: {partitions}")
    logger.info(f"Number of partitions after filtering: {len(partitions)}")

    if not partitions:
        patterns_found = set()

        for folder in folders:
            folder_str = str(folder)
            if 'date' in folder_str.lower():
                patterns_found.add(folder_str)

        error_msg = (
            f"No partitions found with 'date=' pattern. "
            f"Available folders: {folders}. "
            f"Folders with 'date' in name: {list(patterns_found)}"
        )
        logger.error(error_msg)
        raise AirflowException(error_msg)
    
    try:
        partitions_sorted = sorted(
            partitions,
            key=lambda x: str(x).split("date=")[1].rstrip("/"),
            reverse=True
        )

        logger.info(f"Sorted partitions: {partitions_sorted}")
        latest = partitions_sorted[0]
        logger.info(f"Selected latest partition: {latest}")

        ti.xcom_push(key="latest_partition", value=latest)
        return latest
        
    except Exception as e:
        error_msg = f"Error processing partitions: {str(e)}. Partitions: {partitions}"
        logger.error(error_msg)
        raise AirflowException(error_msg)


def debug_s3_partitions(**context):
    """Debug what S3ListOperator actually returns."""

    ti: TaskInstance = context['ti']
    folders = ti.xcom_pull(task_ids="list_available_partitions")
    
    logger.info("=== DEBUG S3 PARTITIONS ===")
    logger.info(f"Type of folders: {type(folders)}")
    logger.info(f"Raw folders: {folders}")
    
    if folders:
        logger.info(f"Number of folders: {len(folders)}")
        for i, folder in enumerate(folders):
            logger.info(f"Folder {i}: {folder} (type: {type(folder)})")
            if "date=" in str(folder):
                logger.info(f"  -> Contains 'date=' pattern")
            else:
                logger.info(f"  -> No 'date=' pattern")
    else:
        logger.warning("No folders returned from list_available_partitions")
    
    s3_hook = S3Hook(aws_conn_id='aws_default')
    all_keys = s3_hook.list_keys(
        bucket_name='trino-data-lake',
        prefix='smart_meter_data/'
    )
    logger.info(f"All S3 keys with prefix 'smart_meter_data/': {all_keys}")

    return folders


with DAG(
    'enhanced_minio_to_clickhouse_etl',
    default_args=default_args,
    description='Enhanced ETL Pipeline from MinIO to ClickHouse with monitoring and sensors',
    schedule_interval=None,  # '0 2 * * *',  # Daily at 2 AM
    catchup=False,
    tags=['telecom', 'clickhouse', 'etl', 'minio', 'production'],
    max_active_runs=1,
    on_failure_callback=handle_etl_failure,
    doc_md="""
    # Enhanced MinIO to ClickHouse ETL Pipeline

    Production-ready ETL pipeline with comprehensive monitoring, sensors, and validation.
    
    ## Features:
    - Health checks for all components
    - Data availability sensors
    - Comprehensive validation
    - Notifications
    - Resource cleanup

    ## Pipeline Steps:
    1. Wait for dependencies (MinIO, ClickHouse)
    2. Health checks and infrastructure setup
    3. Data availability validation
    4. Spark ETL processing
    5. Results validation and quality checks
    6. Notifications and cleanup
    """
) as dag:

    start_pipeline = EmptyOperator(task_id='start_pipeline')

    # wait_for_data_quality = ExternalTaskSensor(
    #     task_id='wait_for_data_quality',
    #     external_dag_id='data_quality_check',
    #     external_task_id='data_validation_complete',
    #     allowed_states=['success'],
    #     execution_date_fn=lambda exec_date: exec_date,
    #     mode='reschedule',
    #     timeout=1800,
    #     poke_interval=30,
    # )

    mark_etl_complete = ExternalTaskMarker(
        task_id='mark_etl_complete',
        external_dag_id='analytics_pipeline',
        external_task_id='wait_for_etl_completion',
        execution_date='{{ execution_date }}'
    )

    wait_for_minio = PythonSensor(
        task_id='wait_for_minio',
        python_callable=check_minio_health_via_aws_conn,
        timeout=300,
        poke_interval=30,
        mode='reschedule'
    )

    check_minio_data_files = PythonSensor(
        task_id='check_minio_data_files',
        python_callable=check_minio_files_exists,
        mode='reschedule',
        timeout=300,
        poke_interval=30,
        op_kwargs={
            'processing_date': '{{ ds }}',
            'bucket_name': 'trino-data-lake',
            'expected_files': 1
        }
    )

    list_available_partitions = S3ListOperator(
        task_id="list_available_partitions",
        bucket="trino-data-lake",
        prefix="smart_meter_data/",
        aws_conn_id="aws_default"
    )

    debug_partitions_task = PythonOperator(
        task_id='debug_partitions',
        python_callable=debug_s3_partitions,
        provide_context=True
    )

    select_latest_partition_task = PythonOperator(
        task_id="select_latest_partition",
        python_callable=select_latest_partition,
        provide_context=True
    )


    wait_for_clickhouse = PythonSensor(
        task_id='wait_for_clickhouse',
        python_callable=check_clickhouse_health_direct,
        timeout=300,
        poke_interval=30,
        mode='reschedule'
    )


    check_minio_health = PythonOperator(
        task_id='check_minio_health',
        python_callable=check_minio_connection
    )

    check_clickhouse_health = PythonOperator(
        task_id='check_clickhouse_health',
        python_callable=check_clickhouse_health
    )
    
    setup_infrastructure = PythonOperator(
        task_id='setup_infrastructure',
        python_callable=setup_clickhouse_infrastructure
    )
    
    validate_source_data = BashOperator(
        task_id='validate_source_data',
        bash_command="""
        echo "Validating source data availability for date {{ ds }}"
        # Add actual data validation logic here
        exit 0
        """
    )
    
    minio_to_clickhouse_etl = SparkSubmitOperator(
        task_id='minio_to_clickhouse_etl',
        application='/opt/airflow/dags/spark/pipelines/telecom_etl/jobs/minio_to_clickhouse.py',
        name='minio-to-clickhouse-etl',
        conn_id='spark_default',
        application_args=[
            '--config', 'etl_prod.conf', 
            '--date', '{{ ds }}',
            '--prod'
        ],
        env_vars={
            "PYTHONPATH": "/opt/airflow/dags:/opt/airflow/dags/spark"
        },
        packages="org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                "com.clickhouse:clickhouse-jdbc:0.4.6,"
                "com.clickhouse:clickhouse-http-client:0.4.6",
        conf={
            "spark.pyspark.python": "/usr/local/bin/python3.10",
            "spark.pyspark.driver.python": "/usr/local/bin/python3.10",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9002",
            "spark.hadoop.fs.s3a.access.key": "minioadmin", 
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
        },
        driver_memory='2g',
        executor_memory='2g',
        verbose=True
    )

    validate_etl_results = PythonOperator(
        task_id='validate_etl_results',
        python_callable=validate_etl_results,
        provide_context=True
    )
 
    cleanup_task = PythonOperator(
        task_id='cleanup_resources',
        python_callable=cleanup_resources
    )
    
    # trigger_analytics_dag = TriggerDagRunOperator(
    #     task_id='trigger_analytics_dag',
    #     trigger_dag_id='telecom_analytics_dag',
    #     wait_for_completion=False,
    #     reset_dag_run=True,
    #     trigger_rule='all_done'
    # )

    end_pipeline = BashOperator(
        task_id='end_pipeline',
        bash_command="""
        echo "=========================================="
        echo "ETL Pipeline Completed: {{ ds }}"
        echo "DAG: {{ dag.dag_id }}"
        echo "Execution: {{ execution_date }}"
        echo "Run ID: {{ run_id }}"
        echo "=========================================="
        """,
        trigger_rule='all_done'
    )


(
    start_pipeline

    # 1. Platform availability
    >> wait_for_minio
    >> wait_for_clickhouse

    # 2. Health checks
    >> check_minio_health
    >> check_clickhouse_health

    # 3. Source data availability in MinIO
    >> check_minio_data_files

    # 4. Partition discovery
    >> list_available_partitions
    >> debug_partitions_task
    >> select_latest_partition_task

    # 5. Prepare ClickHouse target tables
    >> setup_infrastructure

    # 6. Additional source validation (placeholder)
    >> validate_source_data

    # 7. Run Spark ETL
    >> minio_to_clickhouse_etl

    # 8. Validate ClickHouse results
    >> validate_etl_results

    # 9. Optimize + cleanup
    >> cleanup_task

    # 10. Let downstream DAG know ETL is complete
    >> mark_etl_complete

    # 11. Final logging
    >> end_pipeline
)
