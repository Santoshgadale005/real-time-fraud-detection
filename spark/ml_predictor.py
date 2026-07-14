import joblib
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

MODEL_PATH = PROJECT_ROOT / "models" / "fraud_model.pkl"
ENCODER_PATH = PROJECT_ROOT / "models" / "label_encoder.pkl"


class MLPredictor:

    def __init__(self):
        self.model = joblib.load(MODEL_PATH)
        self.encoder = joblib.load(ENCODER_PATH)

    def predict_pandas(self, df: pd.DataFrame):

        if df.empty:
            return df

        df = df.copy()

        # Encode transaction type
        df["type"] = self.encoder.transform(df["type"])

        features = [
            "step",
            "type",
            "amount",
            "oldbalanceOrg",
            "newbalanceOrig",
            "oldbalanceDest",
            "newbalanceDest",
            "isFlaggedFraud",
        ]

        X = df[features]

        prediction = self.model.predict(X)
        probability = self.model.predict_proba(X)[:, 1]

        df["prediction"] = prediction
        df["confidence"] = probability

        return df