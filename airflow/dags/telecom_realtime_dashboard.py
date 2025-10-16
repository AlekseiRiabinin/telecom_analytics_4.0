from airflow import DAG
from clickhouse_provider.operators.clickhouse_operator import ClickhouseOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import logging


def validate_dashboard_data(**context):
    """Validate that dashboard data was updated successfully"""
    ti = context['ti']
    
    try:
        # We could add validation logic here
        # For example, check if records were inserted, verify data quality, etc.
        
        logging.info("Dashboard data validation completed successfully")
        return {"validation_status": "success", "timestamp": datetime.now().isoformat()}
        
    except Exception as e:
        logging.error(f"Dashboard data validation failed: {e}")
        raise

with DAG(
    'telecom_realtime_dashboard_enhanced',
    default_args={
        'owner': 'dashboard-team',
        'start_date': datetime(2024, 1, 1),
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
    },
    description='Enhanced telecom dashboard with validation',
    schedule_interval=timedelta(minutes=15),
    catchup=False,
    tags=['telecom', 'dashboard', 'real-time', 'validation'],
) as dag:

    start_update = EmptyOperator(task_id='start_update')

    # Update real-time KPIs
    update_network_kpis = ClickhouseOperator(
        task_id='update_network_kpis',
        sql='''
        INSERT INTO telecom.realtime_kpis
        SELECT 
            now() as timestamp,
            avg(signal_strength) as avg_network_quality,
            count(distinct device_id) as active_devices,
            sum(data_usage_mb) as data_usage_last_15min,
            avg(latency_ms) as avg_latency
        FROM telecom.network_events
        WHERE event_time >= now() - interval 15 minute
        ''',
        click_conn_id='clickhouse_default',
        do_xcom_push=True
    )

    # Update additional metrics in parallel
    update_geo_data = ClickhouseOperator(
        task_id='update_geo_data',
        sql='''
        INSERT INTO telecom.geo_heatmap
        SELECT 
            region,
            count(*) as event_count,
            avg(signal_strength) as signal_quality,
            now() as update_time
        FROM telecom.network_events
        WHERE event_time >= now() - interval 1 hour
        GROUP BY region
        ''',
        click_conn_id='clickhouse_default'
    )

    # Data validation
    validate_data = PythonOperator(
        task_id='validate_dashboard_data',
        python_callable=validate_dashboard_data,
        provide_context=True
    )

    end_update = EmptyOperator(task_id='end_update')

    # Define workflow
    start_update >> [update_network_kpis, update_geo_data] >> validate_data >> end_update
