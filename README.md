# 🍕 Pizza Orders Data Pipeline

This project streams random pizza orders through Kafka, ingests them into PostgreSQL, and uses dbt to transform and validate the data quality.

# 📂 Project Architecture
Producer: Python script running locally, simulating incoming pizza orders.  
Streaming: Apache Kafka (Dockerized) handles the order queue.  
Orchestration: Apache Airflow schedules and monitors the data flow.  
Ingestion: Python Consumer (via Airflow) reads Kafka offsets and writes to Postgres.  
Transformation: dbt processes raw data into revenue models and analytical views.  
Storage: PostgreSQL acts as the central data warehouse.  

## 📂 Project Structure
```text
project_kafka/
├── dags/                       # Airflow DAG definitions
│   └── pizza_dbt_dag.py        # Main pipeline orchestration
├── dbt_project/                # dbt models and configurations
│   ├── models/                 # SQL transformations (revenue, stats)
│   └── seeds/                  # Static data (pizza_prices.csv)
├── scripts/
│   ├── pizza_producer.py       # Local Python script (Ubuntu host)
│   └── kafka_to_postgress.py   # Containerized ingestion script
├── .env                        # Database & secret credentials
├── docker-compose.yml          # Kafka, Postgres, & Airflow stack
└── Dockerfile                  # Custom Airflow image with dependencies
