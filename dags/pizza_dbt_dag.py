from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'kirill',
    'start_date': datetime(2026, 3, 22)
}

with DAG(
    '02_pizza_dbt_transformation',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
) as dag:

    # Run dbt run
    run_dbt = BashOperator(
        task_id='dbt_run_revenue',
        # We point to the project folder we mounted in docker-compose
        bash_command='cd /opt/airflow/project && dbt run --profiles-dir .dbt'
    )

    # Run dbt test
    test_dbt = BashOperator(
        task_id='dbt_test_quality',
        bash_command='cd /opt/airflow/project && dbt test --profiles-dir .dbt'
    )

    run_dbt >> test_dbt