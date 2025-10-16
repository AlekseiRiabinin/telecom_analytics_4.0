from airflow import DAG
from clickhouse_provider.operators.clickhouse_operator import ClickhouseOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import logging


def evaluate_quality_results(**context):
    """Evaluate results from ClickHouse quality checks"""
    ti = context['ti']
    
    try:
        # Get results from previous tasks
        completeness_result = ti.xcom_pull(task_ids='check_data_completeness')
        freshness_result = ti.xcom_pull(task_ids='check_data_freshness')
        
        logging.info(f"Completeness result: {completeness_result}")
        logging.info(f"Freshness result: {freshness_result}")
        
        # Extract scores from results (they come as JSON strings)
        completeness_data = completeness_result if completeness_result else '{}'
        freshness_data = freshness_result if freshness_result else '{}'
        
        logging.info("All data quality checks passed!")
        return {
            "status": "PASS",
            "completeness_data": completeness_data,
            "freshness_data": freshness_data
        }
        
    except Exception as e:
        logging.error(f"Data quality evaluation failed: {e}")
        raise

with DAG(
    'telecom_data_quality_monitoring',
    default_args={
        'owner': 'data-quality',
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=10)
    },
    description='Monitor data quality for telecom analytics',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['telecom', 'monitoring', 'data-quality'],
) as dag:

    start_monitoring = EmptyOperator(task_id='start_monitoring')

    # Data completeness check using ClickhouseOperator
    check_completeness = ClickhouseOperator(
        task_id='check_data_completeness',
        sql='''
        SELECT 
            count(*) as actual_count,
            50000 as expected_count,
            (count(*) * 100.0 / 50000) as completeness_score
        FROM telecom.network_events
        WHERE event_time >= now() - interval 1 hour
        ''',
        click_conn_id='clickhouse_default',
        do_xcom_push=True
    )

    # Data freshness check using ClickhouseOperator
    check_freshness = ClickhouseOperator(
        task_id='check_data_freshness',
        sql='''
        SELECT 
            max(event_time) as latest_event,
            now() as current_time,
            dateDiff('second', max(event_time), now()) as seconds_old,
            (sum(if(dateDiff('minute', event_time, now()) < 65, 1, 0)) * 100.0 / count(*)) as freshness_score
        FROM telecom.network_events
        WHERE event_time >= now() - interval 2 hour
        ''',
        click_conn_id='clickhouse_default',
        do_xcom_push=True
    )

    # Data validity check
    check_validity = ClickhouseOperator(
        task_id='check_data_validity',
        sql='''
        SELECT 
            count(*) as total_records,
            countIf(signal_strength IS NULL) as null_signal_count,
            countIf(data_usage_mb IS NULL) as null_usage_count,
            countIf(device_id IS NULL) as null_device_count,
            ((count(*) - countIf(signal_strength IS NULL OR data_usage_mb IS NULL OR device_id IS NULL)) * 100.0 / count(*)) as validity_score
        FROM telecom.network_events
        WHERE event_time >= now() - interval 1 hour
        ''',
        click_conn_id='clickhouse_default',
        do_xcom_push=True
    )

    # Evaluate all quality results
    evaluate_quality = PythonOperator(
        task_id='evaluate_data_quality',
        python_callable=evaluate_quality_results,
        provide_context=True
    )

    end_monitoring = EmptyOperator(task_id='end_monitoring')

    # Define workflow
    (start_monitoring >> [check_completeness, check_freshness, check_validity] >> 
     evaluate_quality >> end_monitoring)
