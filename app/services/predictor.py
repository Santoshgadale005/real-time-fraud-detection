import csv
import os
from datetime import datetime

import joblib
import pandas as pd

from config.logger import logger

# Load model and encoder
model = joblib.load("models/fraud_model.pkl")
encoder = joblib.load("models/label_encoder.pkl")


def predict_transaction(data: dict):
    """
    Predict whether a transaction is fraudulent.
    """

    try:
        # Encode transaction type
        transaction_type = encoder.transform([data["type"]])[0]

        # Create dataframe in the same order used during training
        features = pd.DataFrame([{
            "step": data["step"],
            "type": transaction_type,
            "amount": data["amount"],
            "oldbalanceOrg": data["oldbalanceOrg"],
            "newbalanceOrig": data["newbalanceOrig"],
            "oldbalanceDest": data["oldbalanceDest"],
            "newbalanceDest": data["newbalanceDest"],
        }])

        prediction = int(model.predict(features)[0])

        result = {
            "prediction": prediction,
            "label": "Fraud" if prediction == 1 else "Normal"
        }

        # Save prediction log
        os.makedirs("logs", exist_ok=True)
        log_file = "logs/predictions.csv"

        file_exists = os.path.exists(log_file)

        with open(log_file, "a", newline="") as f:
            writer = csv.writer(f)

            if not file_exists:
                writer.writerow([
                    "timestamp",
                    "step",
                    "type",
                    "amount",
                    "prediction"
                ])

            writer.writerow([
                datetime.now(),
                data["step"],
                data["type"],
                data["amount"],
                prediction
            ])

        logger.info(result)

        return result

    except Exception as e:
        logger.exception("Prediction failed")
        return {
            "error": str(e)
        }