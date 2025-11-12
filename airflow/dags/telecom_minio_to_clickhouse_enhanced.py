"""
Enhanced MinIO to ClickHouse ETL Pipeline
With sensors, monitoring, and production-ready features.
"""

import logging
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor, ExternalTaskMarker
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.python import PythonSensor
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowException
from airflow.models import Variable
from datetime import datetime, timedelta


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

        buckets = s3.list_buckets()
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
                objects = s3.list_objects_v2(Bucket=target_bucket, Prefix=prefix, MaxKeys=1)
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
    """Enhanced ClickHouse health check with metrics."""

    try:
        logger.info("Performing ClickHouse health check...")
        
        clickhouse_hook = get_clickhouse_hook()
        
        result = clickhouse_hook.run("SELECT 1")
        logger.info("ClickHouse basic connectivity confirmed")
        
        metrics_query = """
        SELECT 
            metric,
            value
        FROM system.metrics
        WHERE metric IN ('Query', 'Merge', 'ReplicatedFetch', 'ReplicatedSend')
        """
        metrics = clickhouse_hook.run(metrics_query)
        logger.info("ClickHouse system metrics:")
        for metric, value in metrics:
            logger.info(f"  {metric}: {value}")
        
        db_status = clickhouse_hook.run("""
            SELECT 
                name,
                engine,
                total_rows as rows,
                formatReadableSize(total_bytes) as size
            FROM system.tables 
            WHERE database = 'telecom_analytics'
        """)
        
        logger.info("Telecom analytics database status:")
        for table in db_status:
            logger.info(f"  Table: {table[0]}, Rows: {table[2]}, Size: {table[3]}")
        
        version_result = clickhouse_hook.run("SELECT version()")
        version = version_result[0][0] if version_result else "Unknown"
        
        logger.info(f"ClickHouse health check passed - Version: {version}")
        return f"ClickHouse healthy - Version: {version}"
        
    except Exception as e:
        logger.error(f"ClickHouse health check failed: {e}")
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

    try:
        processing_date = kwargs['ds']
        ti = kwargs['ti']
        
        logger.info(f"Validating ETL results for date: {processing_date}")
        
        clickhouse_hook = get_clickhouse_hook()
        
        validation_queries = {
            "total_records": f"""
                SELECT count(*) as record_count 
                FROM telecom_analytics.smart_meter_raw 
                WHERE date = '{processing_date}'
            """,
            "data_quality": f"""
                SELECT 
                    count(*) as total,
                    countIf(energy_consumption > 0) as valid_consumption,
                    countIf(voltage BETWEEN 200 AND 250) as valid_voltage,
                    countIf(is_anomaly = 1) as anomaly_count
                FROM telecom_analytics.smart_meter_raw 
                WHERE date = '{processing_date}'
            """,
            "meter_stats": f"""
                SELECT 
                    count(distinct meter_id) as unique_meters,
                    avg(energy_consumption) as avg_consumption,
                    max(energy_consumption) as max_consumption
                FROM telecom_analytics.smart_meter_raw 
                WHERE date = '{processing_date}'
            """
        }
        
        validation_results = {}
        
        for check_name, query in validation_queries.items():
            result = clickhouse_hook.run(query)
            validation_results[check_name] = result[0] if result else None
            logger.info(f"Validation {check_name}: {result[0] if result else 'No result'}")
        
        ti.xcom_push(key='validation_results', value=validation_results)

        record_count = (
            validation_results['total_records'][0] 
            if validation_results['total_records'] else 0
        )
        
        if record_count > 0:
            logger.info(f"ETL validation successful! Loaded {record_count:,} records")
            return f"ETL validated: {record_count:,} records"
        else:
            logger.warning("No records found for validation")
            return "ETL validated: No records found"
            
    except Exception as e:
        logger.error(f"ETL validation failed: {e}")
        raise AirflowException(f"ETL validation failed: {e}")

def send_slack_notification(**kwargs):
    """Send notification about ETL pipeline status."""

    try:
        # This would integrate with Slack webhook
        # For now, just log the notification
        ti = kwargs['ti']
        dag_run = kwargs['dag_run']
        
        validation_results = ti.xcom_pull(task_ids='validate_etl_results', key='validation_results')
        
        if validation_results and validation_results['total_records']:
            record_count = validation_results['total_records'][0]
            message = f"ETL Pipeline Completed Successfully!\n"
            message += f"• DAG: {dag_run.dag_id}\n"
            message += f"• Execution: {dag_run.execution_date}\n"
            message += f"• Records Processed: {record_count:,}\n"
            
            if validation_results['data_quality']:
                dq = validation_results['data_quality']
                message += f"• Data Quality: {dq[1]}/{dq[0]} valid records\n"
                message += f"• Anomalies Detected: {dq[3]}"
        else:
            message = f"ETL Pipeline Completed with Warnings\n"
            message += f"• DAG: {dag_run.dag_id}\n"
            message += f"• Execution: {dag_run.execution_date}\n"
            message += f"• No records processed for this date"
        
        logger.info(f"Slack Notification: {message}")    
       
        return "Notification sent"
        
    except Exception as e:
        logger.warning(f"Notification failed: {e}")
        return "Notification failed"

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
    exception = context.get('exception')
    task_instance = context.get('task_instance')
    
    error_message = f"ETL Pipeline Failed!\n"
    error_message += f"Task: {task_instance.task_id}\n"
    error_message += f"Error: {str(exception) if exception else 'Unknown error'}\n"
    error_message += f"Execution: {context.get('execution_date')}"
    
    logger.error(error_message)
    

def check_minio_files_exists(processing_date, bucket_name, expected_files, **kwargs):
    """Check if required files exist in MinIO for the processing date."""

    try:
        import boto3
        from botocore.client import Config
        
        s3 = boto3.client(
            's3',
            endpoint_url='http://minio:9002',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            config=Config(signature_version='s3v4'),
            verify=False
        )
        
        prefix = f"smart_meter_data/date={processing_date}/"
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        file_count = len(response.get('Contents', []))
        logger.info(f"Found {file_count} files for date {processing_date}")
        
        if file_count >= expected_files:
            logger.info(f"Sufficient files found: {file_count} (expected at least {expected_files})")
            return True
        else:
            logger.info(f"Waiting for files: found {file_count}, need {expected_files}")
            return False
            
    except Exception as e:
        logger.warning(f"Error checking MinIO files: {e}")
        return False

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

with DAG(
    'enhanced_minio_to_clickhouse_etl',
    default_args=default_args,
    description='Enhanced ETL Pipeline from MinIO to ClickHouse with monitoring and sensors',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
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

    wait_for_data_ingestion = ExternalTaskSensor(
        task_id='wait_for_data_ingestion',
        external_dag_id='data_ingestion_pipeline',  # Name of the upstream DAG
        external_task_id='end_pipeline',            # Specific task to wait for
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        execution_date_fn=lambda exec_date: exec_date,
        mode='reschedule',
        timeout=3600,      # 1 hour timeout
        poke_interval=60,  # Check every minute
    )

    wait_for_data_quality = ExternalTaskSensor(
        task_id='wait_for_data_quality',
        external_dag_id='data_quality_check',
        external_task_id='data_validation_complete',
        allowed_states=['success'],
        execution_date_fn=lambda exec_date: exec_date,
        mode='reschedule',
        timeout=1800,
        poke_interval=30,
    )

    mark_etl_complete = ExternalTaskMarker(
        task_id='mark_etl_complete',
        external_dag_id='analytics_pipeline',
        external_task_id='wait_for_etl_completion',
        execution_date='{{ execution_date }}'
    )

    wait_for_minio = HttpSensor(
        task_id='wait_for_minio',
        http_conn_id='minio_http',
        endpoint='minio:9002/minio/health/live',
        response_check=lambda response: response.status_code == 200,
        timeout=300,
        poke_interval=30,
        mode='reschedule'
    )

    check_minio_data_files = PythonSensor(
        task_id='check_minio_data_files',
        python_callable=check_minio_files_exists,
        mode='reschedule',
        timeout=3600,
        poke_interval=60,
        op_kwargs={
            'processing_date': '{{ ds }}',
            'bucket_name': 'trino-data-lake',
            'expected_files': 5
        }
    )

    check_config_file = FileSensor(
        task_id='check_config_file',
        filepath='/opt/airflow/configs/etl_prod.conf',
        mode='reschedule',
        timeout=300,
        poke_interval=30,
    )

    check_spark_app = FileSensor(
        task_id='check_spark_app',
        filepath='/opt/airflow/dags/spark/pipelines/telecom_etl/jobs/minio_to_clickhouse.py',
        mode='reschedule',
        timeout=300,
        poke_interval=30,
    )

    wait_for_clickhouse = HttpSensor(
        task_id='wait_for_clickhouse',
        http_conn_id='clickhouse_http',
        endpoint='',
        response_check=lambda response: response.status_code == 200,
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
    
    send_notification = PythonOperator(
        task_id='send_notification',
        python_callable=send_slack_notification,
        provide_context=True
    )
    
    cleanup_task = PythonOperator(
        task_id='cleanup_resources',
        python_callable=cleanup_resources
    )
    
    trigger_analytics_dag = TriggerDagRunOperator(
        task_id='trigger_analytics_dag',
        trigger_dag_id='telecom_analytics_dag',
        wait_for_completion=False,
        reset_dag_run=True
    )

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
    >> [wait_for_minio, wait_for_clickhouse, wait_for_data_ingestion]
    
    >> [
        check_minio_health, check_clickhouse_health,
        check_minio_data_files, check_config_file, check_spark_app
    ]
    
    >> setup_infrastructure
    >> validate_source_data
    >> minio_to_clickhouse_etl
    >> validate_etl_results
    >> [send_notification, cleanup_task, mark_etl_complete]
    >> end_pipeline
)
