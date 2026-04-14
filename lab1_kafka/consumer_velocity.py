from kafka import KafkaConsumer
import json
from collections import defaultdict
from datetime import datetime, timedelta

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='velocity-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_events = defaultdict(list)

print("Listening for velocity anomalies...")

for message in consumer:
    tx = message.value
    user_id = tx["user_id"]
    ts = datetime.fromisoformat(tx["timestamp"])

    user_events[user_id].append(ts)

    cutoff = ts - timedelta(seconds=60)
    user_events[user_id] = [t for t in user_events[user_id] if t >= cutoff]

    if len(user_events[user_id]) > 3:
        print(
            f"ALERT: {user_id} made {len(user_events[user_id])} transactions "
            f"within 60 seconds. Last tx: {tx['tx_id']}"
        )
