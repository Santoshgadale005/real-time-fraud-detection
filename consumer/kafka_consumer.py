from kafka import KafkaConsumer
import json
import pandas as pd
import joblib

# Load model and encoder
model = joblib.load("models/fraud_model.pkl")
encoder = joblib.load("models/label_encoder.pkl")

consumer = KafkaConsumer(
    "transactions",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
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