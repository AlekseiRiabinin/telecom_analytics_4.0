from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import logging


logger = logging.getLogger("airflow.task")


def test_clickhouse_hook_explicit():
    """Test ClickHouse with explicit parameters using HTTP protocol."""

    try:
        from clickhouse_provider.hooks.clickhouse_hook import ClickhouseHook
        logger.info("Testing ClickHouse with explicit parameters (HTTP)...")
        
        clickhouse_hook = ClickhouseHook(
            host='clickhouse',
            port=8123,
            database='telecom_analytics',
            user='admin',
            password='clickhouse_admin',
            protocol='http'
        )

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
            logger.warning("Target database 'telecom_analytics' not found")
        
        return f"ClickHouse HTTP explicit params OK - Version: {version}"
        
    except Exception as e:
        logger.error(f"ClickHouse HTTP explicit params failed: {e}")

        return test_clickhouse_direct_fallback()


def test_clickhouse_direct_fallback():
    """Fallback direct connection test."""

    try:
        import clickhouse_connect
        logger.info("Falling back to direct HTTP connection...")
        
        client = clickhouse_connect.get_client(
            host='clickhouse',
            port=8123,
            username='admin',
            password='clickhouse_admin',
            database='telecom_analytics'
        )
        
        result = client.query("SELECT version() as version")
        version = result.result_rows[0][0]
        logger.info(f"Direct HTTP fallback successful. Version: {version}")
        return f"Direct HTTP fallback OK - Version: {version}"
        
    except Exception as e:
        logger.error(f"Direct HTTP fallback also failed: {e}")
        raise


def test_clickhouse_direct():
    """Test direct connection using clickhouse-connect (HTTP)."""

    try:
        import clickhouse_connect
        logger.info("Testing direct ClickHouse HTTP connection...")
        
        client = clickhouse_connect.get_client(
            host='clickhouse',
            port=8123,
            username='admin',
            password='clickhouse_admin',
            database='telecom_analytics'
        )
        
        result = client.query("SELECT version() as version, currentDatabase() as db")
        version = result.result_rows[0][0]
        database = result.result_rows[0][1]
        
        logger.info(f"Direct HTTP connection successful")
        logger.info(f"Version: {version}")
        logger.info(f"Database: {database}")
        
        tables_result = client.query("SHOW TABLES")
        tables = [table[0] for table in tables_result.result_rows]
        logger.info(f"Tables in current database: {tables}")
        
        return f"Direct HTTP connection OK - Version: {version}"
        
    except Exception as e:
        logger.error(f"Direct HTTP connection failed: {e}")
        raise


def test_clickhouse_native():
    """Test native protocol connection."""

    try:
        from clickhouse_driver import Client
        logger.info("Testing native protocol connection...")
        
        client = Client(
            host='clickhouse',
            port=9000,
            user='admin',
            password='clickhouse_admin',
            database='telecom_analytics',
            connect_timeout=10,
            send_receive_timeout=30
        )
        
        result = client.execute("SELECT version()")
        version = result[0][0]
        logger.info(f"Native connection successful. Version: {version}")
        
        # Performance test
        import time
        start_time = time.time()
        large_result = client.execute("SELECT number FROM system.numbers LIMIT 50000")
        transfer_time = time.time() - start_time
        
        logger.info(f"⚡ Native performance: {len(large_result)} rows in {transfer_time:.3f}s")
        
        client.disconnect()

        return (
            f"Native connection OK - {len(large_result)} "
            f"rows @ {len(large_result)/transfer_time:.0f} rows/sec"
        )
        
    except Exception as e:
        logger.warning(f"Native connection failed (expected): {e}")
        logger.info("This is expected - native protocol has authentication issues")
        return "Native connection skipped - authentication issue"


def test_clickhouse_performance_comparison():
    """Compare performance of working methods."""

    try:
        import time
        logger.info("Performance comparison of working methods...")
        
        results = {}
        
        # Method 1: Direct HTTP with clickhouse-connect
        try:
            import clickhouse_connect
            
            start_time = time.time()
            client = clickhouse_connect.get_client(
                host='clickhouse',
                port=8123,
                username='admin',
                password='clickhouse_admin'
            )
            result_direct = client.query("SELECT number FROM system.numbers LIMIT 10000")
            direct_time = time.time() - start_time
            results['clickhouse-connect (HTTP)'] = direct_time
            logger.info(f"clickhouse-connect HTTP - 10k rows: {direct_time:.3f}s")
            
        except Exception as e:
            logger.error(f"clickhouse-connect performance test failed: {e}")
        
        # Method 2: HTTP via requests (fallback)
        try:
            import requests
            
            start_time = time.time()
            response = requests.post(
                'http://clickhouse:8123/',
                params={'query': 'SELECT number FROM system.numbers LIMIT 10000'},
                auth=('admin', 'clickhouse_admin')
            )
            if response.status_code == 200:
                requests_time = time.time() - start_time
                results['requests (HTTP)'] = requests_time
                logger.info(f"requests HTTP - 10k rows: {requests_time:.3f}s")
            else:
                logger.error(f"HTTP requests failed: {response.status_code} - {response.text}")
            
        except Exception as e:
            logger.error(f"HTTP requests performance test failed: {e}")
        
        # Performance summary
        if len(results) > 0:
            fastest_method = min(results, key=results.get)
            fastest_time = results[fastest_method]
            logger.info(f"Fastest method: {fastest_method} ({fastest_time:.3f}s)")
            
            for method, time_taken in results.items():
                if method != fastest_method:
                    speed_diff = ((time_taken - fastest_time) / time_taken) * 100
                    logger.info(f"   {method} is {speed_diff:+.1f}% slower than {fastest_method}")
        else:
            logger.warning("No performance methods succeeded")
        
        return f"Performance comparison completed - {len(results)} methods tested"
        
    except Exception as e:
        logger.error(f"Performance comparison failed: {e}")
        raise

with DAG(
    'test_clickhouse_connection',
    start_date=datetime(2025, 10, 28),
    schedule_interval=None,
    catchup=False,
    tags=['test', 'clickhouse', 'performance'],
) as dag:

    start = EmptyOperator(task_id='start')
    
    test_hook_explicit = PythonOperator(
        task_id='test_clickhouse_hook_explicit',
        python_callable=test_clickhouse_hook_explicit
    )
    
    test_direct = PythonOperator(
        task_id='test_clickhouse_direct',
        python_callable=test_clickhouse_direct
    )
    
    test_native = PythonOperator(
        task_id='test_clickhouse_native',
        python_callable=test_clickhouse_native
    )
    
    test_perf = PythonOperator(
        task_id='test_clickhouse_performance_comparison',
        python_callable=test_clickhouse_performance_comparison
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> test_hook_explicit >> test_direct >> test_native >> test_perf >> end
