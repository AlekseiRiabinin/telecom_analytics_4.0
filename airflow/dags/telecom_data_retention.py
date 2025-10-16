from airflow import DAG
from clickhouse_provider.operators.clickhouse_operator import ClickhouseOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta


with DAG(
    'telecom_comprehensive_data_retention',
    default_args={
        'owner': 'data-ops',
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=30)
    },
    description='Comprehensive data retention with multiple policies',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['telecom', 'retention', 'archiving', 'comprehensive'],
) as dag:

    start_retention = EmptyOperator(task_id='start_retention')

    # Archive raw events older than 30 days
    archive_raw_events = ClickhouseOperator(
        task_id='archive_raw_events',
        sql='''
        INSERT INTO telecom.network_events_archive
        SELECT *
        FROM telecom.network_events
        WHERE event_time < now() - interval 30 days
        ''',
        click_conn_id='clickhouse_default'
    )

    # Delete archived raw events
    delete_archived_events = ClickhouseOperator(
        task_id='delete_archived_events',
        sql='''
        ALTER TABLE telecom.network_events
        DELETE WHERE event_time < now() - interval 30 days
        ''',
        click_conn_id='clickhouse_default'
    )

    # Archive daily metrics older than 90 days
    archive_old_metrics = ClickhouseOperator(
        task_id='archive_old_metrics',
        sql='''
        INSERT INTO telecom.metrics_archive
        SELECT *
        FROM telecom.network_metrics_daily
        WHERE date < today() - interval 90 days
        ''',
        click_conn_id='clickhouse_default'
    )

    # Delete old daily metrics
    delete_old_metrics = ClickhouseOperator(
        task_id='delete_old_metrics',
        sql='''
        ALTER TABLE telecom.network_metrics_daily
        DELETE WHERE date < today() - interval 90 days
        ''',
        click_conn_id='clickhouse_default'
    )

    # Clean up real-time KPIs older than 7 days
    cleanup_old_kpis = ClickhouseOperator(
        task_id='cleanup_old_kpis',
        sql='''
        ALTER TABLE telecom.realtime_kpis
        DELETE WHERE timestamp < now() - interval 7 days
        ''',
        click_conn_id='clickhouse_default'
    )

    # Optimize all tables
    optimize_main_tables = ClickhouseOperator(
        task_id='optimize_main_tables',
        sql='''
        OPTIMIZE TABLE telecom.network_events FINAL;
        OPTIMIZE TABLE telecom.network_metrics_daily FINAL;
        OPTIMIZE TABLE telecom.realtime_kpis FINAL;
        ''',
        click_conn_id='clickhouse_default'
    )

    end_retention = EmptyOperator(task_id='end_retention')

    # Define workflow with parallel execution where possible
    (start_retention >> 
     [archive_raw_events, archive_old_metrics, cleanup_old_kpis] >>
     [delete_archived_events, delete_old_metrics] >>
     optimize_main_tables >> end_retention)
