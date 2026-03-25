FROM apache/airflow:2.8.1

# Switch to root to install system dependencies if needed
USER root
RUN apt-get update && apt-get install -y git && apt-get clean

# Switch back to the airflow user to install python packages
USER airflow

# Install dbt for postgres
RUN pip install --no-cache-dir kafka-python-ng psycopg2-binary dbt-postgres