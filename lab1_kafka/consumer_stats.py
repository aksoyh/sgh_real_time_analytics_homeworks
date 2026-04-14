from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='stats-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

stats = defaultdict(lambda: {
    "count": 0,
    "total": 0.0,
    "min": float("inf"),
    "max": float("-inf")
})

msg_count = 0

print("Tracking category statistics...")

for message in consumer:
    tx = message.value
    category = tx["category"]
    amount = tx["amount"]

    stats[category]["count"] += 1
    stats[category]["total"] += amount
    stats[category]["min"] = min(stats[category]["min"], amount)
    stats[category]["max"] = max(stats[category]["max"], amount)

    msg_count += 1

    if msg_count % 10 == 0:
        print("\n--- CATEGORY STATS ---")
        print("Category | Count | Total Revenue | Min Amount | Max Amount")
        for c in sorted(stats):
            s = stats[c]
            print(f"{c} | {s['count']} | {s['total']:.2f} | {s['min']:.2f} | {s['max']:.2f}")
