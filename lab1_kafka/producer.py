from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

stores = ["Warsaw", "Krakow", "Gdansk", "Wroclaw"]
categories = ["electronics", "clothing", "food", "books"]

def generate_transaction(tx_number):
    return {
        "tx_id": f"TX{tx_number:04d}",
        "user_id": f"u{random.randint(1, 20):02d}",
        "amount": round(random.uniform(5.0, 5000.0), 2),
        "store": random.choice(stores),
        "category": random.choice(categories),
        "timestamp": datetime.now().isoformat()
    }

for i in range(1, 51):
    transaction = generate_transaction(i)
    producer.send("transactions", value=transaction)
    print(f"Sent: {transaction}")
    time.sleep(1)

producer.flush()
print("Done sending 50 transactions.")
