import json
from confluent_kafka import Consumer

config = {
    "bootstrap.servers": "pkc-619z3.us-east1.gcp.confluent.cloud:9092",
    "sasl.mechanisms": "PLAIN",
    "security.protocol": "SASL_SSL",
    "sasl.username": "M4WGH2SK276V55G3",
    "sasl.password": "cfltqPDYB5LMHZlXpsRIkElhC01VNXxCYYyPLQdvP/nv08p5fxg7JXeBoStrIkAQ",
    "group.id": "output-consumer-group",
    "auto.offset.reset": "latest",
}

consumer = Consumer(config)
consumer.subscribe(["predictions"])

print("Output Consumer started. Waiting for predictions...\n")
print(f"{'#':<6} {'Amount':<12} {'Prediction':<20} {'Probability':<14} {'Actual'}")
print("-" * 70)

count = 0
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        result = json.loads(msg.value().decode())
        count += 1
        print(
            f"{count:<6} "
            f"${result['Amount']:<11.2f} "
            f"{result['alert']:<20} "
            f"{result['fraud_probability']:<14} "
            f"Actual={'FRAUD' if result['actual_class']==1 else 'legit'}"
        )
except KeyboardInterrupt:
    print("\nConsumer stopped.")
finally:
    consumer.close()