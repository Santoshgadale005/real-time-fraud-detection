"""spark/ml_predictor.py — Machine Learning Predictor Integration (Day 19).

Loads the trained Isolation Forest model and StandardScaler, preprocesses
incoming transaction features, and runs real-time inference on Spark micro-batches.
"""

import json
import logging
from pathlib import Path
from typing import Any
import joblib
import pandas as pd
import numpy as np

logger = logging.getLogger("spark-ml-predictor")

# Paths to artifacts
PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODEL_PATH = PROJECT_ROOT / "deployment" / "models" / "isolation_forest_v1.pkl"
SCALER_PATH = PROJECT_ROOT / "deployment" / "models" / "scaler_v1.pkl"
FEATURES_PATH = PROJECT_ROOT / "deployment" / "config" / "features.json"


class MLPredictor:
    """Singleton predictor that loads production artifacts once and provides

    methods for scaling and prediction on Pandas DataFrames.
    """
    _model = None
    _scaler = None
    _features = []

    def __init__(self) -> None:
        self._ensure_loaded()

    def _ensure_loaded(self) -> None:
        """Lazy load model, scaler, and features metadata."""
        if MLPredictor._model is None:
            if not MODEL_PATH.exists():
                raise FileNotFoundError(f"Model not found at: {MODEL_PATH}")
            logger.info("Loading production Isolation Forest model: %s", MODEL_PATH)
            MLPredictor._model = joblib.load(MODEL_PATH)
            
        if MLPredictor._scaler is None:
            if not SCALER_PATH.exists():
                raise FileNotFoundError(f"Scaler not found at: {SCALER_PATH}")
            logger.info("Loading production StandardScaler: %s", SCALER_PATH)
            MLPredictor._scaler = joblib.load(SCALER_PATH)
            
        if not MLPredictor._features:
            if not FEATURES_PATH.exists():
                # Fallback to default expected feature list if features.json is missing
                MLPredictor._features = [
                    "step", "amount", "oldbalanceOrg", "newbalanceOrig",
                    "oldbalanceDest", "newbalanceDest",
                    "type_CASH_OUT", "type_DEBIT", "type_PAYMENT", "type_TRANSFER"
                ]
            else:
                with open(FEATURES_PATH, encoding="utf-8") as f:
                    MLPredictor._features = json.load(f)["features"]
            logger.info("Loaded expected feature list: %s", MLPredictor._features)

    def predict_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply feature scaling, run model prediction, and return prediction results.

        This method acts as the core of Day 19 Step 6/8/9/10/11:
        1. Aligns and orders features exactly as specified in features.json.
        2. Scales the features using the StandardScaler.
        3. Predicts anomalies (-1 for anomaly, 1 for normal).
        4. Calculates decision function anomaly scores.
        5. Converts predictions to binary (1 = Fraud, 0 = Normal).
        """
        if df.empty:
            return df

        # Ensure correct column ordering
        # Missing features are initialized to 0.0 to prevent scikit-learn errors
        ordered_cols = MLPredictor._features
        for col_name in ordered_cols:
            if col_name not in df.columns:
                logger.warning("Feature '%s' missing from input micro-batch. Defaulting to 0.0.", col_name)
                df[col_name] = 0.0

        # Create named DataFrame in exact feature ordering (preserves sklearn feature names)
        X = df[ordered_cols].reset_index(drop=True)

        # Scale features — pass a DataFrame so sklearn sees named columns (no warning)
        X_scaled = pd.DataFrame(
            MLPredictor._scaler.transform(X),
            columns=ordered_cols,
        )

        # Generate raw predictions (-1 or 1) & anomaly scores
        raw_predictions = MLPredictor._model.predict(X_scaled)
        anomaly_scores = MLPredictor._model.decision_function(X_scaled)

        # Convert predictions: -1 (anomaly) -> 1 (fraud), 1 (normal) -> 0 (normal)
        binary_predictions = np.where(raw_predictions == -1, 1, 0)

        # Attach results to the original DataFrame
        df["prediction"] = binary_predictions
        df["anomaly_score"] = anomaly_scores
        
        return df
