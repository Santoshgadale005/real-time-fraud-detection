import joblib
import pandas as pd

# Load model and label encoder only once when the API starts
model = joblib.load("models/fraud_model.pkl")
encoder = joblib.load("models/label_encoder.pkl")


def predict_transaction(transaction):
    """
    Predict whether a transaction is fraudulent.
    """

    # Convert input dictionary to DataFrame
    df = pd.DataFrame([transaction])

    # Encode transaction type
    df["type"] = encoder.transform(df["type"])

    # Make prediction
    prediction = model.predict(df)[0]

    # Get confidence score
    confidence = max(model.predict_proba(df)[0])

    return {
        "prediction": "Fraud" if prediction == 1 else "Legitimate",
        "confidence": round(float(confidence), 4)
    }