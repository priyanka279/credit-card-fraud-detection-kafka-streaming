import pandas as pd
import json
import time
from confluent_kafka import Producer
from config import KAFKA_CONFIG

producer = Producer(KAFKA_CONFIG)
df = pd.read_csv("data/creditcard.csv")

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Sent row {msg.key().decode()} to '{msg.topic()}'")

print("Producer started. Sending rows to 'raw-data'...")

for _, row in df.iterrows():
    record = row.drop("Time").to_dict()
    producer.produce(
        "raw-data",
        key=str(row.name),
        value=json.dumps(record),
        callback=delivery_report
    )
    producer.poll(0)
    time.sleep(1)

producer.flush()