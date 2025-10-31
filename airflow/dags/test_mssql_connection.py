from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import logging

logger = logging.getLogger("airflow.task")

def test_mssql_connectivity():
    """Test MSSQL server connectivity"""
    import pyodbc
    logger.info("🔌 Testing MSSQL connectivity...")
    
    try:
        # Test connection using pyodbc
        conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            "SERVER=mssql,1433;"
            "DATABASE=telecom_db;"
            "UID=sa;"
            "PWD=Admin123!;"
            "TrustServerCertificate=yes;"
        )
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Test simple query
        cursor.execute("SELECT 1 as test_value")
        result = cursor.fetchone()
        
        logger.info(f"✅ MSSQL connection successful: {result[0]}")
        cursor.close()
        conn.close()
        return "MSSQL connectivity OK"
        
    except Exception as e:
        logger.error(f"❌ MSSQL connection failed: {e}")
        raise

def test_pymssql_connectivity():
    """Test alternative pymssql connection"""
    try:
        import pymssql
        logger.info("🔌 Testing pymssql connectivity...")
        
        conn = pymssql.connect(
            server='mssql',
            port=1433,
            user='sa',
            password='Admin123!',
            database='telecom_db'
        )
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        result = cursor.fetchone()
        
        logger.info("✅ pymssql connection successful")
        cursor.close()
        conn.close()
        return "pymssql connectivity OK"
        
    except Exception as e:
        logger.error(f"❌ pymssql connection failed: {e}")
        raise

with DAG(
    'test_mssql_connection',
    start_date=datetime(2025, 10, 28),
    schedule_interval=None,
    catchup=False,
    tags=['test', 'mssql'],
) as dag:

    start = EmptyOperator(task_id='start')
    
    test_pyodbc = PythonOperator(
        task_id='test_pyodbc_connection',
        python_callable=test_mssql_connectivity
    )
    
    test_pymssql = PythonOperator(
        task_id='test_pymssql_connection',
        python_callable=test_pymssql_connectivity
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> test_pyodbc >> test_pymssql >> end
