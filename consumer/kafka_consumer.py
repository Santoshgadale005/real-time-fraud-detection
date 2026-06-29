from kafka import KafkaConsumer
import json
import pandas as pd
import joblib
import os

# Load model and encoder
model = joblib.load("models/fraud_model.pkl")
encoder = joblib.load("models/label_encoder.pkl")

# Kafka server
bootstrap_server = os.getenv("KAFKA_BOOTSTRAP_SERVER", "kafka:29092")

# Create Kafka Consumer
consumer = KafkaConsumer(
    "transactions",
    bootstrap_servers=bootstrap_server,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="fraud-consumer",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Kafka Consumer Started...")

for message in consumer:
    transaction = message.value

    df = pd.DataFrame([transaction])

    # Remove columns not used during training
    df.drop(columns=["nameOrig", "nameDest"], errors="ignore", inplace=True)

    # Encode transaction type
    if "type" in df.columns:
        df["type"] = encoder.transform(df["type"])

    # Remove target column if present
    X = df.drop(columns=["isFraud"], errors="ignore")

    prediction = model.predict(X)[0]
    probability = model.predict_proba(X)[0].max()

    print("=" * 50)
    print("Prediction:", "Fraud" if prediction == 1 else "Legitimate")
    print("Confidence:", round(probability, 4))