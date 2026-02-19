import json
from kafka import KafkaConsumer

#Initilize the Consumer
consumer = KafkaConsumer(
    'pizza_orders',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("--- Kitchen Dashboard: Waiting for Orders ---")
for message in consumer:
    order = message.value
    print(f"🍕 Kitchen received: Order #{order['order_id']} - {order['pizza']}")