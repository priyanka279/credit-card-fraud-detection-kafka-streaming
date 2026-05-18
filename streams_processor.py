import faust
import json
import joblib
import numpy as np
import ssl

ssl_context = ssl.create_default_context()

app = faust.App(
    "fraud-detector",
    broker="kafka://pkc-619z3.us-east1.gcp.confluent.cloud:9092",
    broker_credentials=faust.SASLCredentials(
        username=KAFKA_CONFIG["sasl.username"],
        password=KAFKA_CONFIG["sasl.password"],
        mechanism="PLAIN",
        ssl_context=ssl_context,
    ),
    value_serializer="raw",
    topic_replication_factor=3,
    topic_partitions=1,
)

raw_topic = app.topic("raw-data", value_type=bytes)
predictions_topic = app.topic("predictions", value_type=bytes)

model = joblib.load("fraud_model.joblib")
FEATURE_COLS = [f"V{i}" for i in range(1, 29)] + ["Amount"]

@app.agent(raw_topic)
async def process(stream):
    async for message in stream:
        try:
            record = json.loads(message)
            features = np.array([[record[col] for col in FEATURE_COLS]])
            prediction = model.predict(features)[0]
            probability = model.predict_proba(features)[0][1]

            result = {
                "Amount": record["Amount"],
                "actual_class": record.get("Class"),
                "predicted_fraud": int(prediction),
                "fraud_probability": round(float(probability), 4),
                "alert": "FRAUD DETECTED" if prediction == 1 else "Legitimate"
            }

            await predictions_topic.send(value=json.dumps(result).encode())
            print(f"Processed: {result['alert']} | Amount: ${record['Amount']:.2f}")

        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    app.main()