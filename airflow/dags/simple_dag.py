from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime


with DAG(
    'simple_test_dag',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 1, 1),
    },
    description='A simple test DAG',
    schedule_interval=None,
    catchup=False,
    tags=['test'],
) as dag:

    task1 = EmptyOperator(task_id='task1')
    task2 = EmptyOperator(task_id='task2')
    task3 = EmptyOperator(task_id='task3')

    task1 >> task2 >> task3
