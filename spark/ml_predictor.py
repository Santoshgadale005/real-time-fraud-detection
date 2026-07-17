"""spark/ml_predictor.py — ML Predictor using IsolationForest (Day 19/21 production).

Loads the trained Isolation Forest production model and StandardScaler once
at startup and provides a predict_pandas() method for per-batch inference.
"""

import logging
import joblib
import pandas as pd
import numpy as np
from pathlib import Path

logger = logging.getLogger("spark-ml-predictor")

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Use the production IsolationForest + its matching scaler
MODEL_PATH  = PROJECT_ROOT / "models" / "production_model.pkl"
SCALER_PATH = PROJECT_ROOT / "deployment" / "models" / "scaler_v1.pkl"
# Fallback scaler location
SCALER_FALLBACK = PROJECT_ROOT / "models" / "scaler.pkl"

# Feature columns that the IsolationForest was trained on (order matters)
FEATURE_COLS = [
    "step", "amount", "oldbalanceOrg", "newbalanceOrig",
    "oldbalanceDest", "newbalanceDest",
    "type_CASH_OUT", "type_DEBIT", "type_PAYMENT", "type_TRANSFER",
]


class MLPredictor:
    """Singleton-style predictor that loads model/scaler once."""

    def __init__(self) -> None:
        logger.info("Loading production IsolationForest model from: %s", MODEL_PATH)
        self.model = joblib.load(MODEL_PATH)

        # Try primary scaler path, then fallback
        if SCALER_PATH.exists():
            logger.info("Loading scaler from: %s", SCALER_PATH)
            self.scaler = joblib.load(SCALER_PATH)
        elif SCALER_FALLBACK.exists():
            logger.info("Loading scaler (fallback) from: %s", SCALER_FALLBACK)
            self.scaler = joblib.load(SCALER_FALLBACK)
        else:
            logger.warning("No scaler found — scaling will be skipped.")
            self.scaler = None

        logger.info("MLPredictor ready. Model: %s", type(self.model).__name__)

    def predict_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Run IsolationForest inference on a pandas DataFrame.

        Returns the original DataFrame with two extra columns:
          - prediction   : 1 = fraud, 0 = normal
          - anomaly_score: raw decision function score (more negative = more anomalous)
        """
        if df.empty:
            return df

        df = df.copy()

        # One-hot encode transaction type (if not already done by Spark preprocessing)
        if "type" in df.columns and "type_CASH_OUT" not in df.columns:
            for t in ["CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]:
                df[f"type_{t}"] = (df["type"] == t).astype(int)

        # Select only the feature columns that exist
        available = [c for c in FEATURE_COLS if c in df.columns]
        missing   = [c for c in FEATURE_COLS if c not in df.columns]
        if missing:
            logger.warning("Missing feature columns — filling with 0: %s", missing)
            for c in missing:
                df[c] = 0

        X = df[FEATURE_COLS].astype(float)

        # Apply scaler if available
        if self.scaler is not None:
            try:
                X_scaled = self.scaler.transform(X)
                X = pd.DataFrame(X_scaled, columns=FEATURE_COLS, index=X.index)
            except Exception as e:
                logger.warning("Scaler transform failed (%s) — using raw features.", e)

        # IsolationForest: predict() returns -1 (anomaly) or 1 (normal)
        raw_pred = self.model.predict(X)
        scores   = self.model.decision_function(X)

        # Convert: -1 → 1 (fraud), 1 → 0 (normal)
        df["prediction"]   = np.where(raw_pred == -1, 1, 0)
        df["anomaly_score"] = scores

        return df