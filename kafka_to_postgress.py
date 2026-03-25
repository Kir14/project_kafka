import os
import json
import psycopg2
from kafka import KafkaConsumer

#Configuration
TOPIC_NAME = 'pizza_orders'
GROUP_ID = 'checkpoint_pizza_orders'

print(f"DB_NAME={os.getenv('DB_NAME')}")
# 1. Connection to Postgres
conn = psycopg2.connect(
    host="postgres",
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    port="5432"
)
cursor = conn.cursor()

# Create table if it doesn't exist
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS raw_pizza_orders (
        id SERIAL PRIMARY KEY
        , order_id INT
        , pizza_type TEXT
        , order_status TEXT
        , received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
)
conn.commit()

# 2. Setup Kafka Consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=['kafka:9092'],
    group_id=GROUP_ID,
    enable_auto_commit=False,
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=100
)

print("🚀 Bridge Active: Moving data from Kafka to Postgres...")

try:
    for message in consumer:
        order = message.value

        # SQL Insert
        cursor.execute(
            f"""
                INSERT INTO raw_pizza_orders (order_id, pizza_type, order_status)
                VALUES (%s, %s, %s)
            """,
            (order['order_id'], order['pizza'], order['status'])
        )
        conn.commit()
        consumer.commit()

        print(f"✅ Saved Order #{order['order_id']} to Database")
except Exception as e:
    print(f"Error: {e}")
    conn.rollback() # Undo DB changes if something failed
finally:
    cursor.close()
    conn.close()
    consumer.close()
    print("Closed connections. Airflow task finishing...")