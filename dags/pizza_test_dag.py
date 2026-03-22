from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'kirill',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    '01_pizza_health_check',
    default_args=default_args,
    description='Checks if Postgres is alive and dbt can see the project',
    start_date=datetime(2026,3,22),
    schedule_interval='@hourly',
    catchup=False,
    tags=['pizza_project']
) as dag:

    # 1. Test: Can Airflow talk to Postgres?
    check_postgres = PostgresOperator(
        task_id='check_postgres_connection',
        postgres_conn_id='postgres_default',
        sql='SELECT 1;'
    )

    # 2. Test: Can Airflow see dbt files
    check_files = BashOperator(
        task_id='list_project_files',
        bash_command='ls -R /opt/airflow/project'
    )

    check_postgres >> check_files

