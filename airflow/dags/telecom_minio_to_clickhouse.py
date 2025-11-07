"""
MinIO to ClickHouse ETL Pipeline
Using ClickHouse Hook with explicit HTTP parameters (proven working approach)
"""

import logging
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import time


logger = logging.getLogger("airflow.task")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False,
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


def check_clickhouse_connection():
    """Check ClickHouse connection."""
    
    try:
        logger.info("Testing ClickHouse connection with explicit HTTP parameters...")
        
        clickhouse_hook = get_clickhouse_hook()
        result = clickhouse_hook.run("SELECT version() as version")
        version = result[0][0] if result else "Unknown"
        logger.info(f"ClickHouse HTTP connection successful. Version: {version}")
        
        databases = clickhouse_hook.run("SHOW DATABASES")
        database_names = [db[0] for db in databases]
        logger.info(f"Available databases: {database_names}")
        
        if 'telecom_analytics' in database_names:
            logger.info("Target database 'telecom_analytics' exists")
            
            tables = clickhouse_hook.run("SHOW TABLES FROM telecom_analytics")
            table_names = [table[0] for table in tables]
            logger.info(f"Tables in telecom_analytics: {table_names}")
        else:
            logger.info("Creating telecom_analytics database...")
            clickhouse_hook.run("CREATE DATABASE IF NOT EXISTS telecom_analytics")
            logger.info("Created telecom_analytics database")
        
        return f"ClickHouse connection OK - Version: {version}"
        
    except Exception as e:
        logger.error(f"ClickHouse connection failed: {e}")
        raise AirflowException(f"ClickHouse connection failed: {e}")


def create_clickhouse_tables():
    """Create optimized ClickHouse tables."""
    
    try:
        logger.info("Creating/verifying ClickHouse tables...")
        
        clickhouse_hook = get_clickhouse_hook()
        
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
                total_energy_daily Float32,
                avg_energy_daily Float32,
                avg_voltage_daily Float32,
                avg_current_daily Float32,
                max_consumption_daily Float32,
                min_consumption_daily Float32,
                std_energy_daily Float32,
                record_count_daily UInt32,
                anomaly_count_daily UInt32,
                total_energy_meter Float32,
                avg_energy_meter Float32,
                peak_consumption Float32,
                total_readings UInt32,
                active_days UInt16,
                total_anomalies UInt32,
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
                if "already exists" in str(e):
                    logger.info(f"Table {table_name} already exists")
                else:
                    logger.warning(f"Table {table_name} creation note: {e}")
        
        tables_result = clickhouse_hook.run("SHOW TABLES FROM telecom_analytics")
        table_names = [table[0] for table in tables_result]
        logger.info(f"Final tables in telecom_analytics: {table_names}")
        
        return "ClickHouse tables created/verified successfully"
        
    except Exception as e:
        logger.error(f"Failed to create ClickHouse tables: {e}")
        raise AirflowException(f"Table creation failed: {e}")


def load_sample_data():
    """Load sample data for testing and validation."""
    
    try:
        logger.info("Loading sample data for testing...")
        
        clickhouse_hook = get_clickhouse_hook()
        
        sample_data = [
            "('meter_001', '2024-01-15 10:00:00', 15.5, 220.0, 10.2, 0.95, 50.0, 'MEDIUM', 0)",
            "('meter_001', '2024-01-15 10:15:00', 16.2, 219.5, 10.5, 0.96, 50.0, 'MEDIUM', 0)", 
            "('meter_002', '2024-01-15 10:00:00', 12.8, 221.0, 9.8, 0.94, 50.0, 'MEDIUM', 0)",
            "('meter_002', '2024-01-15 10:15:00', 13.1, 220.5, 10.0, 0.95, 50.0, 'MEDIUM', 0)",
            "('meter_003', '2024-01-15 10:00:00', 8.5, 218.0, 8.2, 0.93, 50.0, 'LOW', 0)",
            "('meter_003', '2024-01-15 10:15:00', 28.7, 223.0, 12.1, 0.97, 50.0, 'HIGH', 1)"
        ]
        
        insert_query = """
        INSERT INTO telecom_analytics.smart_meter_raw (
            meter_id, timestamp, energy_consumption, voltage, 
            current_reading, power_factor, frequency, consumption_category, is_anomaly
        ) VALUES 
        """ + ", ".join(sample_data)
        
        clickhouse_hook.run(insert_query)
        logger.info("Sample data loaded successfully")
        
        count_result = clickhouse_hook.run("SELECT count(*) FROM telecom_analytics.smart_meter_raw")
        record_count = count_result[0][0] if count_result else 0
        logger.info(f"Total records in smart_meter_raw: {record_count}")
        
        sample_result = clickhouse_hook.run("""
            SELECT meter_id, timestamp, energy_consumption, consumption_category, is_anomaly 
            FROM telecom_analytics.smart_meter_raw 
            ORDER BY timestamp 
            LIMIT 5
        """)
        logger.info("🔍 Sample of loaded data:")
        for row in sample_result:
            logger.info(f"   {row}")
        
        return f"Sample data loaded - {record_count} total records"
        
    except Exception as e:
        logger.error(f"Failed to load sample data: {e}")
        raise AirflowException(f"Sample data loading failed: {e}")


def create_materialized_views():
    """Create materialized views for real-time aggregations."""
    
    try:
        logger.info("Creating materialized views for real-time analytics...")
        
        clickhouse_hook = get_clickhouse_hook()
        
        hourly_mv_ddl = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS telecom_analytics.hourly_consumption_mv
        ENGINE = AggregatingMergeTree()
        PARTITION BY toYYYYMM(hour)
        ORDER BY (meter_id, hour)
        AS SELECT
            meter_id,
            toStartOfHour(timestamp) as hour,
            sumState(energy_consumption) as total_energy,
            avgState(energy_consumption) as avg_energy,
            countState() as readings_count,
            sumState(is_anomaly) as anomalies_count
        FROM telecom_analytics.smart_meter_raw
        GROUP BY meter_id, hour
        """
        
        daily_mv_ddl = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS telecom_analytics.daily_consumption_mv  
        ENGINE = AggregatingMergeTree()
        PARTITION BY toYYYYMM(date)
        ORDER BY (meter_id, date)
        AS SELECT
            meter_id,
            date,
            sumState(energy_consumption) as total_energy,
            avgState(energy_consumption) as avg_energy,
            countState() as readings_count,
            sumState(is_anomaly) as anomalies_count
        FROM telecom_analytics.smart_meter_raw
        GROUP BY meter_id, date
        """
        
        clickhouse_hook.run(hourly_mv_ddl)
        logger.info("Created hourly_consumption_mv materialized view")
        
        clickhouse_hook.run(daily_mv_ddl)
        logger.info("Created daily_consumption_mv materialized view")
        
        views_result = clickhouse_hook.run("""
            SELECT name FROM system.tables 
            WHERE database = 'telecom_analytics' AND engine = 'MaterializedView'
        """)
        view_names = [view[0] for view in views_result]
        logger.info(f"Materialized views created: {view_names}")
        
        return "Materialized views created successfully"
        
    except Exception as e:
        logger.error(f"Failed to create materialized views: {e}")
        raise AirflowException(f"Materialized views creation failed: {e}")


def verify_etl_results(**kwargs):
    """Verify ETL results in ClickHouse."""
    
    try:
        logger = logging.getLogger(__name__)
        
        processing_date = kwargs['ds']
        logger.info(f"Verifying ETL results for date: {processing_date}")
        
        clickhouse_hook = get_clickhouse_hook()
        
        count_query = (
            f"SELECT count(*) FROM telecom_analytics.smart_meter_raw "
            f"WHERE date = '{processing_date}'"
        )
        
        logger.info(f"Executing query: {count_query}")
        
        count_result = clickhouse_hook.run(count_query)
        logger.info(f"Raw count result: {count_result}")
        
        if count_result and len(count_result) == 2:
            data, columns = count_result
            record_count = data[0][0] if data and len(data) > 0 else 0
            
            logger.info(f"ETL verification successful!")
            logger.info(f"Records loaded: {record_count:,}")
            
            if record_count > 0:
                return f"ETL verified: {record_count:,} records loaded"
            else:
                logger.warning("No records found for the specified date")
                return "ETL verified: No records found"
        else:
            logger.warning("Unexpected result format")
            return "ETL verified: Check completed"
            
    except Exception as e:
        logger.error(f"ETL verification failed: {e}")
        raise AirflowException(f"ETL verification failed: {e}")


def wait_for_clickhouse():
    """ClickHouse health check."""
    
    max_retries = 12
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            logger.info(f"ClickHouse health check... (attempt {attempt + 1}/{max_retries})")
            clickhouse_hook = get_clickhouse_hook()
            
            result = clickhouse_hook.run("SELECT 1")
            logger.info(f"ClickHouse connectivity confirmed. Result type: {type(result)}")
            
            version_result = clickhouse_hook.run("SELECT version()")
            if version_result and len(version_result) == 2:
                version_data, _ = version_result
                version = version_data[0][0] if version_data else "Unknown"
            else:
                version = "Unknown"
            
            logger.info(f"ClickHouse version: {version}")
            return f"ClickHouse healthy - Version: {version}"
                
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Health check failed: {e}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Health checks failed after {max_retries} attempts")
                raise AirflowException(f"ClickHouse health checks failed: {e}")
    
    raise AirflowException(f"ClickHouse health checks failed after {max_retries} attempts")


def cleanup_old_data():
    """Optional: Cleanup old data to manage storage."""
    
    try:
        logger.info("Checking for old data cleanup...")
        
        clickhouse_hook = get_clickhouse_hook()
        
        partitions_query = """
        SELECT 
            partition,
            count() as parts,
            sum(rows) as total_rows,
            formatReadableSize(sum(bytes)) as size
        FROM system.parts 
        WHERE database = 'telecom_analytics' AND table = 'smart_meter_raw'
        GROUP BY partition
        ORDER BY partition
        """
        
        partitions_result = clickhouse_hook.run(partitions_query)
        logger.info("Current partitions:")
        for partition in partitions_result:
            logger.info(f"   Partition {partition[0]}: {partition[2]} rows, {partition[3]}")
        
        cleanup_query = """
        ALTER TABLE telecom_analytics.smart_meter_raw 
        DELETE WHERE partition_date < today() - 90
        """
        clickhouse_hook.run(cleanup_query)
        logger.info("Old data cleanup scheduled")
        
        return "Cleanup check completed"
        
    except Exception as e:
        logger.warning(f"Cleanup check completed with warnings: {e}")
        return f"Cleanup check: {e}"


with DAG(
    'minio_to_clickhouse_etl',
    default_args=default_args,
    description='ETL Pipeline from MinIO to ClickHouse using proven ClickHouse Hook',
    schedule_interval=None, # '0 2 * * *' -> Daily at 2 AM
    catchup=False,
    tags=['telecom', 'clickhouse', 'etl', 'minio'],
    max_active_runs=1,
    doc_md="""
    # MinIO to ClickHouse ETL Pipeline
    
    This DAG uses the proven ClickHouse Hook approach with explicit HTTP parameters
    that was successfully tested in the connection test DAG.
    
    ## Pipeline Steps:
    1. Wait for ClickHouse to be ready
    2. Check connection and create database if needed
    3. Create optimized tables
    4. Load sample data for testing
    5. Create materialized views for real-time analytics
    6. Execute Spark job to transfer data from MinIO
    7. Verify ETL results
    8. Optional cleanup of old data
    """
) as dag:

    start = EmptyOperator(task_id='start_pipeline')
    
    wait_for_db = PythonOperator(
        task_id='wait_for_clickhouse',
        python_callable=wait_for_clickhouse
    )
    
    check_connection = PythonOperator(
        task_id='check_clickhouse_connection',
        python_callable=check_clickhouse_connection
    )
    
    create_tables = PythonOperator(
        task_id='create_clickhouse_tables',
        python_callable=create_clickhouse_tables
    )
    
    load_samples = PythonOperator(
        task_id='load_sample_data',
        python_callable=load_sample_data
    )
    
    create_views = PythonOperator(
        task_id='create_materialized_views',
        python_callable=create_materialized_views
    )

    minio_to_clickhouse = SparkSubmitOperator(
        task_id='minio_to_clickhouse_job',
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
    
    verify_results = PythonOperator(
        task_id='verify_etl_results',
        python_callable=verify_etl_results,
        provide_context=True
    )
    
    cleanup = PythonOperator(
        task_id='cleanup_old_data',
        python_callable=cleanup_old_data
    )
    
    end = EmptyOperator(task_id='end_pipeline')

    (
        start >> wait_for_db >> check_connection >> create_tables >> 
        load_samples >> create_views >> minio_to_clickhouse >> 
        verify_results >> cleanup >> end
    )
