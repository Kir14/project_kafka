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
    run_seed = BashOperator(
        task_id='dbt_run_seed',
        # We point to the project folder we mounted in docker-compose
        bash_command='''
            cd /opt/airflow/project && \
            export DBT_LOG_PATH=/tmp/dbt_logs && \
            export DBT_TARGET_PATH=/tmp/dbt_target && \
            dbt seed --profiles-dir .dbt
        '''
    )

    run_revenue = BashOperator(
        task_id='dbt_run_revenue',
        # We point to the project folder we mounted in docker-compose
        bash_command='''
            cd /opt/airflow/project && \
            export DBT_LOG_PATH=/tmp/dbt_logs && \
            export DBT_TARGET_PATH=/tmp/dbt_target && \
            dbt run -s revenue_by_pizza --profiles-dir .dbt
        '''
    )

    # Run dbt test
    test_dbt = BashOperator(
        task_id='dbt_test_quality',
        bash_command='''
            cd /opt/airflow/project && \
            export DBT_LOG_PATH=/tmp/dbt_logs && \
            export DBT_TARGET_PATH=/tmp/dbt_target && \
            dbt test --profiles-dir .dbt
        '''
    )

    run_seed >> run_revenue >> test_dbt