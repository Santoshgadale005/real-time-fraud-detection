import os
import csv
from datetime import datetime

import joblib
import pandas as pd

from config.logger import logger

# ---------------------------------------------------------
# Load model and encoder
# ---------------------------------------------------------
model = joblib.load("models/fraud_model.pkl")
encoder = joblib.load("models/label_encoder.pkl")

# ---------------------------------------------------------
# Create logs directory
# ---------------------------------------------------------
os.makedirs("logs", exist_ok=True)

LOG_FILE = "logs/prediction_logs.csv"

# ---------------------------------------------------------
# Create CSV log file if not exists
# ---------------------------------------------------------
if not os.path.exists(LOG_FILE):
    with open(LOG_FILE, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([
            "Timestamp",
            "Transaction_ID",
            "Prediction",
            "Confidence",
            "Model_Version"
        ])


# ---------------------------------------------------------
# Prediction Function
# ---------------------------------------------------------
def predict_transaction(transaction):
    try:
        logger.info("Received transaction for prediction")

        df = pd.DataFrame([transaction])

        # ---------------------------
        # Encode type safely
        # ---------------------------
        try:
            df["type"] = encoder.transform(df["type"])
        except Exception:
            raise ValueError(f"Unknown transaction type: {transaction['type']}")

        # ---------------------------
        # Force correct feature order (IMPORTANT FIX)
        # ---------------------------
        expected_features = [
            "step",
            "type",
            "amount",
            "oldbalanceOrg",
            "newbalanceOrig",
            "oldbalanceDest",
            "newbalanceDest",
            "isFlaggedFraud"
        ]

        df = df[expected_features]

        # ---------------------------
        # Prediction
        # ---------------------------
        prediction = model.predict(df)[0]
        proba = model.predict_proba(df)[0]

        # FIXED confidence calculation
        confidence = float(max(proba))

        prediction_text = "Fraud" if prediction == 1 else "Legitimate"

        transaction_id = transaction.get("transaction_id", "N/A")

        logger.info(
            f"Prediction Completed | "
            f"Transaction={transaction_id} | "
            f"Prediction={prediction_text} | "
            f"Confidence={confidence:.4f}"
        )

        # ---------------------------
        # Logging
        # ---------------------------
        with open(LOG_FILE, "a", newline="") as file:
            writer = csv.writer(file)
            writer.writerow([
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                transaction_id,
                prediction_text,
                round(confidence, 4),
                "fraud_model_v1"
            ])

        return {
            "prediction": prediction_text,
            "confidence": round(confidence, 4)
        }

    except Exception as e:
        logger.error(f"Prediction Failed: {str(e)}")

        return {
            "prediction": "Error",
            "confidence": 0.0,
            "message": str(e)
        }