# Real-Time Fraud Detection with Apache Kafka + Faust

## Overview

A real-time streaming pipeline that reads credit card transactions, streams them through Apache Kafka, applies a pre-trained fraud detection ML model using the Faust Streams API, and publishes predictions to an output topic, demonstrating a complete, live ML inference pipeline.


## Dataset

**Credit Card Fraud Detection** — Kaggle  
🔗 https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud

| Property | Detail |
|---|---|
| Total Transactions | 284,807 |
| Fraud Cases | 492 (0.17%) |
| Features | V1–V28 (PCA), Amount, Class |
| ML Task | Binary classification - detect fraud |


## Streams Library

**Option A: Python + Faust (`faust-streaming==0.11.3`)**

The streams processor uses Faust's `@app.agent` decorator to define an async streaming agent. It consumes from the `raw-data` topic, runs the pre-trained ML model on each message, and produces results to the `predictions` topic.

> This is the Faust agent-based streaming model - not a plain consumer loop.

---

## ML Model

| Property | Detail |
|---|---|
| Algorithm | Random Forest Classifier (scikit-learn) |
| Training Split | 80% train / 20% test |
| Training Samples | 227,845 |
| Model File | `fraud_model.joblib` |

### Performance

| Metric | Legitimate (Class 0) | Fraud (Class 1) |
|---|---|---|
| Precision | 1.00 | 0.96 |
| Recall | 1.00 | 0.76 |
| F1-Score | 1.00 | 0.85 |
| Support | 56,864 | 98 |

- **Overall Accuracy:** 100%
- **Macro F1:** 0.92
- **Fraud F1 Score:** 0.8457

The model is trained offline, saved as `fraud_model.joblib`, and loaded once at processor startup. Every incoming Kafka message receives a prediction.

---

## Pipeline Architecture

```
┌─────────────────┐      ┌──────────────┐     ┌────────────────────┐      ┌───────────────┐     ┌──────────────────┐
│   producer.py   │────▶│   raw-data   │────▶│ streams_processor  │────▶│ predictions   │────▶│output_consumer.py│
│                 │     │ (Kafka Topic)│      │      (Faust)       │      │ (Kafka Topic) │     │                  │
│ Reads CSV rows  │     │              │      │  @app.agent loop   │      │               │     │ Prints results   │
│ 1 row/second    │     │              │      │  Runs ML model     │      │               │     │ live to console  │
│ as JSON         │     │              │      │  Produces result   │      │               │     │                  │
└─────────────────┘     └──────────────┘      └────────────────────┘      └───────────────┘     └──────────────────┘
```

---

## Project Structure

```
kafka_streaming/
├── data/
│   └── creditcard.csv          # Dataset (not pushed to GitHub)
├── venv_faust/                 # Python 3.11 virtual environment
├── config.py                   # Confluent Cloud credentials
├── train_model.py              # Offline model training script
├── fraud_model.joblib          # Saved trained model
├── producer.py                 # Kafka producer - streams CSV rows
├── streams_processor.py        # Faust streams processor - ML inference
├── output_consumer.py          # Kafka consumer - prints predictions
├── requirements.txt            # Python dependencies
└── README.md
```

## Setup

### Prerequisites

- Python 3.11
- Confluent Cloud account (free tier)
- Two Kafka topics created: `raw-data` and `predictions`

### 1. Create virtual environment with Python 3.11

```bash
py -3.11 -m venv venv_faust
venv_faust\Scripts\activate
```

### 2. Install dependencies

```bash
pip install faust-streaming==0.11.3 aiokafka==0.11.0 kafka-python==2.0.2
pip install scikit-learn pandas confluent-kafka joblib numpy
```

### 3. Configure credentials

Edit `config.py` and fill in your Confluent Cloud bootstrap server, API key, and API secret.

### 4. Train the model (already done — skip if `fraud_model.joblib` exists)

```bash
python train_model.py
```

---

## How to Run

Open **3 terminals** in this exact order:

### Terminal 1 - Output Consumer *(start first)*

```bash
python output_consumer.py
```

### Terminal 2 - Faust Streams Processor *(start second)*

```bash
venv_faust\Scripts\activate
python -m faust -A streams_processor worker -l info
```

> Wait ~10 seconds for Faust to authenticate and connect before starting Terminal 3.

### Terminal 3 - Producer *(start last)*

```bash
venv_faust\Scripts\activate
python producer.py
```

Predictions will appear in Terminal 1 within a few seconds.

---

## Video Demo

🎥 [Watch Kafka Streaming Demo](https://drive.google.com/file/d/1GL_qzzGLY2pfJ4F-y4RCg6E4OrMKzB3B/view?usp=sharing)

---

## Sample Output

### Producer Output (`producer.py`)

```bash
Sent row 15 to 'raw-data'
Sent row 16 to 'raw-data'
Sent row 17 to 'raw-data'
Sent row 18 to 'raw-data'
Sent row 19 to 'raw-data'
Sent row 20 to 'raw-data'
```

### Faust Streams Processor Output (`streams_processor.py`)

```bash
Processed: Legitimate | Amount: $6.14
Processed: Legitimate | Amount: $27.50
Processed: Legitimate | Amount: $58.80
Processed: Legitimate | Amount: $15.99
Processed: Legitimate | Amount: $12.99
```

### Consumer Output (`output_consumer.py`)

```bash
#   Amount    Prediction    Probability    Actual
14  $27.50    Legitimate    0.0            Actual=Legit
15  $58.80    Legitimate    0.0            Actual=Legit
16  $15.99    Legitimate    0.0            Actual=Legit
17  $12.99    Legitimate    0.0            Actual=Legit
18  $0.89     Legitimate    0.0            Actual=Legit
```

---

## Real-Time Pipeline Behaviour

- Producer streams credit card transactions into Kafka in real time
- Faust agent asynchronously consumes messages from the `raw-data` topic
- Random Forest ML model performs live fraud prediction
- Predictions are published to the `predictions` Kafka topic
- Output consumer displays predictions instantly in the terminal
