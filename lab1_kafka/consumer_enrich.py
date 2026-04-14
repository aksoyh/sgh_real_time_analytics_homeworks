from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='enrich-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Listening and enriching transactions...")

for message in consumer:
    tx = message.value
    amount = tx["amount"]

    if amount > 3000:
        tx["risk_level"] = "HIGH"
    elif amount > 1000:
        tx["risk_level"] = "MEDIUM"
    else:
        tx["risk_level"] = "LOW"

    print(tx)
