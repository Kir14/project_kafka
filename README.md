# 🍕 Pizza Orders Data Pipeline

This project streams random pizza orders through Kafka, ingests them into PostgreSQL, and uses dbt to transform and validate the data quality.

# 📂 Project Architecture
1. Producer: Streams JSON pizza orders to Kafka.
2. Kafka: Distributed message broker (Docker).
3. Ingestion: Python script (kafka_to_postgress.py) moving data to SQL.
4. dbt (Transformation): SQL models to calculate revenue and business metrics.
5. Data Quality: Automated testing suite to ensure data integrity.

## 📂 Project Structure

```text
.
├── docker-compose.yml       # Kafka, Zookeeper, Postgres
├── producer.py              # Data Generator
├── kafka_to_postgress.py    # Stream Ingestion
├── dbt_project.yml          # dbt Config
├── models/
│   ├── marts/               # Business Logic (Revenue, etc.)
│   └── schema.yml           # Data quality tests
├── seeds/                   # Dictionaries
└── requirements.txt         # Python dependencies
