from airflow import DAG
from clickhouse_provider.operators.clickhouse_operator import ClickhouseOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import logging


def evaluate_anomalies(**context):
    """Evaluate detected anomalies and trigger alerts"""
    ti = context['ti']
    
    try:
        # Get anomaly detection results
        statistical_anomalies = ti.xcom_pull(task_ids='detect_statistical_anomalies')
        volume_anomalies = ti.xcom_pull(task_ids='detect_volume_anomalies')
        performance_anomalies = ti.xcom_pull(task_ids='detect_performance_anomalies')
        
        logging.info("Evaluating detected anomalies...")
        
        total_anomalies = 0
        anomaly_summary = {
            "statistical_anomalies": len(statistical_anomalies) if statistical_anomalies else 0,
            "volume_anomalies": len(volume_anomalies) if volume_anomalies else 0,
            "performance_anomalies": len(performance_anomalies) if performance_anomalies else 0,
            "evaluation_time": datetime.now().isoformat()
        }
        
        total_anomalies = (anomaly_summary["statistical_anomalies"] + 
                          anomaly_summary["volume_anomalies"] + 
                          anomaly_summary["performance_anomalies"])
        
        if total_anomalies > 0:
            logging.warning(f"Detected {total_anomalies} total anomalies:")
            logging.warning(f"   - Statistical outliers: {anomaly_summary['statistical_anomalies']}")
            logging.warning(f"   - Volume anomalies: {anomaly_summary['volume_anomalies']}")
            logging.warning(f"   - Performance anomalies: {anomaly_summary['performance_anomalies']}")
            
            # Here you could trigger alerts (Slack, PagerDuty, email, etc.)
            # send_alert(f"Anomalies detected: {total_anomalies}")
        else:
            logging.info("No anomalies detected - system is healthy")
        
        anomaly_summary["total_anomalies"] = total_anomalies
        anomaly_summary["alert_triggered"] = total_anomalies > 0
        
        return anomaly_summary
        
    except Exception as e:
        logging.error(f"Anomaly evaluation failed: {e}")
        raise

def log_anomaly_report(**context):
    """Log comprehensive anomaly report"""
    ti = context['ti']
    anomaly_summary = ti.xcom_pull(task_ids='evaluate_anomalies')
    
    logging.info("ANOMALY DETECTION REPORT:")
    logging.info(f"   Total anomalies: {anomaly_summary.get('total_anomalies', 0)}")
    logging.info(f"   Statistical outliers: {anomaly_summary.get('statistical_anomalies', 0)}")
    logging.info(f"   Volume anomalies: {anomaly_summary.get('volume_anomalies', 0)}")
    logging.info(f"   Performance anomalies: {anomaly_summary.get('performance_anomalies', 0)}")
    logging.info(f"   Evaluation time: {anomaly_summary.get('evaluation_time', 'Unknown')}")
    
    return anomaly_summary

with DAG(
    'telecom_anomaly_detection',
    default_args={
        'owner': 'ml-ops',
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=10),
        'email_on_failure': True
    },
    description='Detect anomalies in telecom network data using statistical methods',
    schedule_interval=timedelta(hours=2),
    catchup=False,
    tags=['telecom', 'anomaly-detection', 'monitoring', 'ml'],
) as dag:

    start_detection = EmptyOperator(task_id='start_detection')

    # Calculate statistical baselines for anomaly detection
    calculate_baselines = ClickhouseOperator(
        task_id='calculate_statistical_baselines',
        sql='''
        INSERT INTO telecom.anomaly_baselines
        SELECT 
            device_type,
            avg(signal_strength) as avg_signal,
            stddevPop(signal_strength) as signal_stddev,
            avg(data_usage_mb) as avg_usage,
            stddevPop(data_usage_mb) as usage_stddev,
            avg(latency_ms) as avg_latency,
            stddevPop(latency_ms) as latency_stddev,
            now() as calculated_at
        FROM telecom.network_metrics_daily
        WHERE date >= today() - 7  -- Use last 7 days for baseline
        GROUP BY device_type
        ''',
        click_conn_id='clickhouse_default'
    )

    # Detect statistical outliers (Z-score method)
    detect_statistical_anomalies = ClickhouseOperator(
        task_id='detect_statistical_anomalies',
        sql='''
        INSERT INTO telecom.detected_anomalies
        SELECT 
            e.device_id,
            e.event_time,
            'z_score_outlier' as anomaly_type,
            abs(e.signal_strength - b.avg_signal) / NULLIF(b.signal_stddev, 0) as signal_z_score,
            abs(e.data_usage_mb - b.avg_usage) / NULLIF(b.usage_stddev, 0) as usage_z_score,
            abs(e.latency_ms - b.avg_latency) / NULLIF(b.latency_stddev, 0) as latency_z_score,
            now() as detected_at
        FROM telecom.network_events e
        JOIN telecom.anomaly_baselines b ON e.device_type = b.device_type
        WHERE e.event_time >= now() - interval 2 hour
          AND (
            abs(e.signal_strength - b.avg_signal) / NULLIF(b.signal_stddev, 0) > 3.0 OR  -- 3 sigma rule
            abs(e.data_usage_mb - b.avg_usage) / NULLIF(b.usage_stddev, 0) > 3.0 OR
            abs(e.latency_ms - b.avg_latency) / NULLIF(b.latency_stddev, 0) > 3.0
          )
        ''',
        click_conn_id='clickhouse_default',
        do_xcom_push=True
    )

    # Detect volume anomalies (unusually high event counts)
    detect_volume_anomalies = ClickhouseOperator(
        task_id='detect_volume_anomalies',
        sql='''
        INSERT INTO telecom.detected_anomalies
        SELECT 
            device_id,
            now() as event_time,
            'volume_anomaly' as anomaly_type,
            event_count as current_volume,
            avg_volume,
            (event_count * 100.0 / avg_volume) as volume_percentage,
            now() as detected_at
        FROM (
            SELECT 
                device_id,
                count(*) as event_count,
                (SELECT avg(daily_count) 
                 FROM (SELECT count(*) as daily_count 
                       FROM telecom.network_events 
                       WHERE event_time >= today() - 7 
                       GROUP BY device_id, toDate(event_time))
                ) as avg_volume
            FROM telecom.network_events
            WHERE event_time >= now() - interval 1 hour
            GROUP BY device_id
            HAVING event_count > avg_volume * 2  -- More than 2x average volume
        )
        ''',
        click_conn_id='clickhouse_default',
        do_xcom_push=True
    )

    # Detect performance degradation anomalies
    detect_performance_anomalies = ClickhouseOperator(
        task_id='detect_performance_anomalies',
        sql='''
        INSERT INTO telecom.detected_anomalies
        SELECT 
            device_id,
            event_time,
            'performance_degradation' as anomaly_type,
            signal_strength,
            data_usage_mb,
            latency_ms,
            CASE 
                WHEN signal_strength < 10 THEN 'CRITICAL'
                WHEN signal_strength < 20 THEN 'HIGH'
                WHEN latency_ms > 500 THEN 'HIGH'
                ELSE 'MEDIUM'
            END as severity,
            now() as detected_at
        FROM telecom.network_events
        WHERE event_time >= now() - interval 1 hour
          AND (
            signal_strength < 20 OR  -- Poor signal strength
            latency_ms > 300 OR      -- High latency
            data_usage_mb > 1000     -- Unusually high data usage
          )
        ''',
        click_conn_id='clickhouse_default',
        do_xcom_push=True
    )

    # Detect geographic anomalies (regional issues)
    detect_geographic_anomalies = ClickhouseOperator(
        task_id='detect_geographic_anomalies',
        sql='''
        INSERT INTO telecom.detected_anomalies
        SELECT 
            'REGION_' || region as device_id,
            now() as event_time,
            'geographic_anomaly' as anomaly_type,
            avg_signal,
            total_events,
            (avg_signal * 100.0 / overall_avg_signal) as signal_percentage,
            now() as detected_at
        FROM (
            SELECT 
                region,
                avg(signal_strength) as avg_signal,
                count(*) as total_events,
                (SELECT avg(signal_strength) 
                 FROM telecom.network_events 
                 WHERE event_time >= now() - interval 1 hour) as overall_avg_signal
            FROM telecom.network_events
            WHERE event_time >= now() - interval 1 hour
            GROUP BY region
            HAVING avg_signal < overall_avg_signal * 0.7  -- 30% worse than average
        )
        ''',
        click_conn_id='clickhouse_default'
    )

    # Evaluate all detected anomalies
    evaluate_anomalies = PythonOperator(
        task_id='evaluate_anomalies',
        python_callable=evaluate_anomalies,
        provide_context=True
    )

    # Log comprehensive report
    log_report = PythonOperator(
        task_id='log_anomaly_report',
        python_callable=log_anomaly_report,
        provide_context=True
    )

    end_detection = EmptyOperator(task_id='end_detection')

    # Define workflow
    (start_detection >> calculate_baselines >> 
     [detect_statistical_anomalies, detect_volume_anomalies, 
      detect_performance_anomalies, detect_geographic_anomalies] >> 
     evaluate_anomalies >> log_report >> end_detection)
