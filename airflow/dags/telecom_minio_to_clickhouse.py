"""
MinIO to ClickHouse ETL Pipeline
Uses ClickHouse Hook for efficient data transfer.
"""

import logging
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta


logger = logging.getLogger("airflow.task")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 28),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'depends_on_past': False,
}


def check_clickhouse_connection():
    """Check ClickHouse connection using the efficient ClickHouse Hook."""
    
    try:
        from clickhouse_provider.hooks.clickhouse_hook import ClickhouseHook
        
        logger.info("Testing ClickHouse connection with ClickHouse Hook...")
        
        clickhouse_hook = ClickhouseHook(
            host='clickhouse-server',
            port=8123,
            database='telecom_analytics',
            user='admin',
            password='clickhouse_admin',
            protocol='http'
        )

        result = clickhouse_hook.run("SELECT version() as version")
        version = result[0][0] if result else "Unknown"
        logger.info(f"ClickHouse connection successful. Version: {version}")
        
        databases = clickhouse_hook.run("SHOW DATABASES")
        database_names = [db[0] for db in databases]
        
        if 'telecom_analytics' in database_names:
            logger.info("Target database 'telecom_analytics' exists")
            
            tables = clickhouse_hook.run("SHOW TABLES FROM telecom_analytics")
            table_names = [table[0] for table in tables]
            logger.info(f"Tables in telecom_analytics: {table_names}")
        else:
            logger.warning("Target database 'telecom_analytics' not found")
        
        return f"ClickHouse connection OK - Version: {version}"
        
    except Exception as e:
        logger.error(f"ClickHouse connection failed: {e}")
        raise AirflowException(f"ClickHouse connection failed: {e}")


def create_clickhouse_tables():
    """Create optimized ClickHouse tables if they don't exist."""
    
    try:
        from clickhouse_provider.hooks.clickhouse_hook import ClickhouseHook
        
        logger.info("Creating/verifying ClickHouse tables...")
        
        clickhouse_hook = ClickhouseHook(
            host='clickhouse-server',
            port=8123,
            database='telecom_analytics',
            user='admin',
            password='clickhouse_admin',
            protocol='http'
        )

        raw_table_ddl = """
        CREATE TABLE IF NOT EXISTS smart_meter_raw (
            meter_id String,
            timestamp DateTime64(3),
            date Date,
            year UInt16,
            month UInt8,
            day UInt8,
            hour UInt8,
            minute UInt8,
            energy_consumption Float32,
            voltage Float32,
            current_reading Float32,
            power_factor Float32,
            frequency Float32,
            consumption_category String,
            is_anomaly UInt8,
            partition_date Date,
            processed_at DateTime64(3)
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(partition_date)
        ORDER BY (meter_id, timestamp)
        SETTINGS index_granularity = 8192
        """
        
        aggregates_ddl = """
        CREATE TABLE IF NOT EXISTS meter_aggregates (
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
        
        mv_daily_ddl = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS daily_consumption_mv
        ENGINE = SummingMergeTree()
        PARTITION BY toYYYYMM(date)
        ORDER BY (meter_id, date)
        AS SELECT
            meter_id,
            date,
            sum(energy_consumption) as total_daily_energy,
            avg(energy_consumption) as avg_daily_energy,
            count(*) as daily_readings,
            sum(is_anomaly) as daily_anomalies
        FROM smart_meter_raw
        GROUP BY meter_id, date
        """
        
        clickhouse_hook.run(raw_table_ddl)
        logger.info("Created/verified smart_meter_raw table")
        
        clickhouse_hook.run(aggregates_ddl)
        logger.info("Created/verified meter_aggregates table")
        
        clickhouse_hook.run(mv_daily_ddl)
        logger.info("Created/verified daily_consumption_mv materialized view")
        
        tables = clickhouse_hook.run("SHOW TABLES")
        table_names = [table[0] for table in tables]
        logger.info(f"Final tables in database: {table_names}")
        
        return "ClickHouse tables created/verified successfully"
        
    except Exception as e:
        logger.error(f"Failed to create ClickHouse tables: {e}")
        raise AirflowException(f"Table creation failed: {e}")


def load_sample_data():
    """Load sample data for testing using ClickHouse Hook."""
    
    try:
        from clickhouse_provider.hooks.clickhouse_hook import ClickhouseHook
        
        logger.info("Loading sample data for testing...")
        
        clickhouse_hook = ClickhouseHook(
            host='clickhouse-server',
            port=8123,
            database='telecom_analytics',
            user='admin',
            password='clickhouse_admin',
            protocol='http'
        )

        sample_data = [
            ('meter_001', '2024-01-15 10:00:00', 15.5, 220.0, 10.2, 0.95, 50.0),
            ('meter_001', '2024-01-15 10:15:00', 16.2, 219.5, 10.5, 0.96, 50.0),
            ('meter_002', '2024-01-15 10:00:00', 12.8, 221.0, 9.8, 0.94, 50.0),
            ('meter_002', '2024-01-15 10:15:00', 13.1, 220.5, 10.0, 0.95, 50.0),
        ]
        
        insert_query = """
        INSERT INTO smart_meter_raw (
            meter_id, timestamp, energy_consumption, voltage, 
            current_reading, power_factor, frequency, date, partition_date
        ) VALUES
        """
        
        values = []
        for row in sample_data:
            values.append(
                f"('{row[0]}', '{row[1]}', {row[2]}, {row[3]}, "
                f"{row[4]}, {row[5]}, {row[6]}, toDate('{row[1]}'), "
                f"toDate('{row[1]}'))"
            )

        if values:
            full_query = insert_query + ", ".join(values)
            clickhouse_hook.run(full_query)
            logger.info(f"Loaded {len(sample_data)} sample records")
        
        count_result = clickhouse_hook.run("SELECT count(*) FROM smart_meter_raw")
        record_count = count_result[0][0] if count_result else 0
        logger.info(f"Total records in smart_meter_raw: {record_count}")
        
        return f"Sample data loaded - {record_count} total records"
        
    except Exception as e:
        logger.error(f"Failed to load sample data: {e}")
        raise AirflowException(f"Sample data loading failed: {e}")


def verify_etl_results(**context):
    """Verify ETL results after Spark job completion."""
    
    try:
        from clickhouse_provider.hooks.clickhouse_hook import ClickhouseHook
        
        execution_date: datetime = context['execution_date']
        processing_date = execution_date.strftime('%Y-%m-%d')

        logger.info(f"Verifying ETL results for date: {processing_date}")

        clickhouse_hook = ClickhouseHook(
            host='clickhouse-server',
            port=8123,
            database='telecom_analytics',
            user='admin',
            password='clickhouse_admin',
            protocol='http'
        )

        raw_count_query = f"""
        SELECT count(*) as record_count 
        FROM smart_meter_raw 
        WHERE date = '{processing_date}'
        """
        raw_count_result = clickhouse_hook.run(raw_count_query)
        raw_count = raw_count_result[0][0] if raw_count_result else 0
        
        agg_count_query = f"""
        SELECT count(*) as agg_count 
        FROM meter_aggregates 
        WHERE date = '{processing_date}'
        """
        agg_count_result = clickhouse_hook.run(agg_count_query)
        agg_count = agg_count_result[0][0] if agg_count_result else 0
        
        quality_query = f"""
        SELECT 
            count(*) as total_records,
            countIf(is_anomaly = 1) as anomaly_count,
            avg(energy_consumption) as avg_consumption,
            min(energy_consumption) as min_consumption,
            max(energy_consumption) as max_consumption
        FROM smart_meter_raw 
        WHERE date = '{processing_date}'
        """
        quality_result = clickhouse_hook.run(quality_query)
        
        logger.info("ETL Verification Results:")
        logger.info(f"   Raw records for {processing_date}: {raw_count}")
        logger.info(f"   Aggregate records: {agg_count}")
        
        if quality_result and quality_result[0]:
            quality_data = quality_result[0]
            logger.info(f"   Data Quality Metrics:")
            logger.info(f"     - Total records: {quality_data[0]}")
            logger.info(f"     - Anomalies: {quality_data[1]}")
            logger.info(f"     - Avg consumption: {quality_data[2]:.2f}")
            logger.info(f"     - Min/Max consumption: {quality_data[3]:.2f}/{quality_data[4]:.2f}")
        
        if raw_count > 0:
            logger.info("ETL verification successful")
            return f"ETL verified: {raw_count} raw records, {agg_count} aggregates"
        else:
            logger.warning("No data found for the processing date")
            return "ETL verified: No data found for processing date"
            
    except Exception as e:
        logger.error(f"ETL verification failed: {e}")
        raise AirflowException(f"ETL verification failed: {e}")


def cleanup_old_data():
    """Cleanup old data to manage storage (optional)."""
    
    try:
        from clickhouse_provider.hooks.clickhouse_hook import ClickhouseHook
        
        logger.info("Cleaning up data older than 30 days...")
        
        clickhouse_hook = ClickhouseHook(
            host='clickhouse-server',
            port=8123,
            database='telecom_analytics',
            user='admin',
            password='clickhouse_admin',
            protocol='http'
        )

        cleanup_query = """
        ALTER TABLE smart_meter_raw 
        DELETE WHERE partition_date < today() - 30
        """
        
        clickhouse_hook.run(cleanup_query)
        logger.info("Old data cleanup scheduled")
        
        # Optimize table after deletion
        optimize_query = "OPTIMIZE TABLE smart_meter_raw FINAL"
        clickhouse_hook.run(optimize_query)
        logger.info("Table optimization completed")
        
        return "Cleanup completed successfully"
        
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        return f"Cleanup warning: {e}"


with DAG(
    'minio_to_clickhouse_etl',
    default_args=default_args,
    description='ETL Pipeline from MinIO to ClickHouse',
    schedule_interval=None,  # '0 2 * * *' Daily at 2 AM
    catchup=False,
    tags=['telecom', 'clickhouse', 'etl', 'minio'],
    max_active_runs=1,
    doc_md="""
    # MinIO to ClickHouse ETL Pipeline
    
    This DAG transfers and transforms data from MinIO to ClickHouse with:
    - Efficient ClickHouse Hook for database operations
    - Spark for large-scale data processing
    - Data quality checks and aggregations
    - Automated table management
    - Result verification
    """
) as dag:

    start = EmptyOperator(task_id='start_pipeline')
    
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
        packages="org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                "com.clickhouse:clickhouse-jdbc:0.4.6,"
                "com.clickhouse:clickhouse-http-client:0.4.6,"
                "com.clickhouse:clickhouse-client:0.4.6",
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
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
        },
        driver_memory='2g',
        executor_memory='2g',
        executor_cores=2,
        num_executors=2,
        verbose=True,
        env_vars={
            'PYTHONPATH': '/opt/airflow/dags:/opt/airflow/dags/spark/pipelines',
            'PYSPARK_PYTHON': '/usr/local/bin/python3.10',
            'PYSPARK_DRIVER_PYTHON': '/usr/local/bin/python3.10'
        }
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

    start >> check_connection >> create_tables >> load_samples >> minio_to_clickhouse
    minio_to_clickhouse >> verify_results >> cleanup >> end
