import json
import psycopg2
from kafka import KafkaConsumer

# 1. Connection to Postgres
conn = psycopg2.connect(
    host="localhost",
    database="pizza_db",
    user="admin",
    password="pass123"
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
    'pizza_orders',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
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

        print(f"✅ Saved Order #{order['order_id']} to Database")
except KeyboardInterrupt:
    cursor.close()
    conn.close()