import json
import time
import random
from kafka import KafkaProducer

#Initilaze the Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8') # Convert dict to JSON
)

orders = ["Margherita", "Pepperoni", "Hawaiian", "Veggie"]

print("--- Start Producer: Sending Orders ---")
while True:
    order_id = random.randint(1000, 9999)
    data = {
        "order_id": order_id,
        "pizza": random.choice(orders),
        "status": "Placed"
    }

    #Send data to 'pizza_orders' topic
    producer.send('pizza_orders', value=data)
    print(f"Send: {data}")
    time.sleep(2)