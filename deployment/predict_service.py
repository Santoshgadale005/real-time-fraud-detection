"""Spark-Ready Prediction Service — Day 14.

This service wraps the Isolation Forest inference pipeline in a form that is
ready to be called from Apache Spark Structured Streaming UDFs.

Design Principles
-----------------
- Loads versioned artifacts (isolation_forest_v1.pkl / scaler_v1.pkl)
- Stateless prediction: each call is fully self-contained
- DataFrame-based inference: preserves feature names, no sklearn warnings
- Thread-safe: singleton artifact loading with lazy initialization
- Schema-validated: rejects malformed data before prediction

Spark Integration Pattern
-------------------------
    from deployment.predict_service import FraudPredictionService
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, BooleanType, StringType

    service = FraudPredictionService()

    OUTPUT_SCHEMA = StructType([
        StructField("prediction",    IntegerType(), False),
        StructField("anomaly_score", FloatType(),   False),
        StructField("is_fraud",      BooleanType(), False),
        StructField("label",         StringType(),  False),
    ])

    @udf(returnType=OUTPUT_SCHEMA)
    def fraud_udf(step, amount, oldbalanceOrg, newbalanceOrig,
                  oldbalanceDest, newbalanceDest,
                  type_CASH_OUT, type_DEBIT, type_PAYMENT, type_TRANSFER):
        txn = {
            "step": step, "amount": amount,
            "oldbalanceOrg": oldbalanceOrg, "newbalanceOrig": newbalanceOrig,
            "oldbalanceDest": oldbalanceDest, "newbalanceDest": newbalanceDest,
            "type_CASH_OUT": type_CASH_OUT, "type_DEBIT": type_DEBIT,
            "type_PAYMENT": type_PAYMENT, "type_TRANSFER": type_TRANSFER,
        }
        result = service.predict(txn)
        return (result["prediction"], result["anomaly_score"],
                result["is_fraud"], result["label"])
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("fraud-predict-service")

# ---------------------------------------------------------------------------
# Paths — versioned artifacts
# ---------------------------------------------------------------------------
SERVICE_DIR  = Path(__file__).resolve().parent
MODEL_PATH   = SERVICE_DIR / "models" / "isolation_forest_v1.pkl"
SCALER_PATH  = SERVICE_DIR / "models" / "scaler_v1.pkl"
FEATURES_PATH = SERVICE_DIR / "config" / "features.json"
METADATA_PATH = SERVICE_DIR / "config" / "model_metadata.json"

# ---------------------------------------------------------------------------
# Feature Schema (loaded from features.json)
# ---------------------------------------------------------------------------
def _load_feature_list() -> list[str]:
    if FEATURES_PATH.exists():
        with open(FEATURES_PATH, encoding="utf-8") as f:
            return json.load(f)["features"]
    # Fallback if config not found
    return [
        "step", "amount", "oldbalanceOrg", "newbalanceOrig",
        "oldbalanceDest", "newbalanceDest",
        "type_CASH_OUT", "type_DEBIT", "type_PAYMENT", "type_TRANSFER",
    ]

FEATURES = _load_feature_list()


class FraudPredictionService:
    """Stateless, Spark-compatible fraud prediction service.

    Loads versioned Isolation Forest model and StandardScaler on first use,
    then caches them for the lifetime of the process (singleton pattern).
    """

    _model  = None
    _scaler = None
    _metadata: dict = {}

    def __init__(self) -> None:
        self._ensure_loaded()

    def _ensure_loaded(self) -> None:
        """Lazy-load model and scaler (thread-safe for single-process use)."""
        if FraudPredictionService._model is None:
            if not MODEL_PATH.exists():
                raise FileNotFoundError(
                    f"Versioned model not found: {MODEL_PATH}\n"
                    "Run deployment setup to create deployment/models/isolation_forest_v1.pkl"
                )
            logger.info("Loading model from %s ...", MODEL_PATH)
            FraudPredictionService._model = joblib.load(MODEL_PATH)
            logger.info("✅ Model loaded: %s", type(FraudPredictionService._model).__name__)

        if FraudPredictionService._scaler is None:
            if not SCALER_PATH.exists():
                raise FileNotFoundError(f"Versioned scaler not found: {SCALER_PATH}")
            logger.info("Loading scaler from %s ...", SCALER_PATH)
            FraudPredictionService._scaler = joblib.load(SCALER_PATH)
            logger.info("✅ Scaler loaded: %s", type(FraudPredictionService._scaler).__name__)

        if not FraudPredictionService._metadata and METADATA_PATH.exists():
            with open(METADATA_PATH, encoding="utf-8") as f:
                FraudPredictionService._metadata = json.load(f)
            logger.info(
                "✅ Metadata loaded: %s v%s",
                FraudPredictionService._metadata.get("model_name", "unknown"),
                FraudPredictionService._metadata.get("model_version", "unknown"),
            )

    def validate(self, transaction: dict[str, Any]) -> dict[str, Any]:
        """Validate that transaction has correct features and types.

        Returns dict with:
            valid    (bool)  — True if the transaction passes all checks
            errors   (list)  — Error messages (empty if valid)
            warnings (list)  — Non-fatal notices
        """
        errors:   list[str] = []
        warnings: list[str] = []

        incoming = set(transaction.keys())
        expected = set(FEATURES)

        missing = expected - incoming
        if missing:
            errors.append(f"Missing features: {sorted(missing)}")

        extra = incoming - expected
        if extra:
            warnings.append(f"Extra features ignored: {sorted(extra)}")

        for feat in FEATURES:
            if feat in transaction:
                val = transaction[feat]
                if not isinstance(val, (int, float, np.integer, np.floating)):
                    errors.append(
                        f"Feature '{feat}': expected numeric, got {type(val).__name__}"
                    )

        return {"valid": len(errors) == 0, "errors": errors, "warnings": warnings}

    def predict(self, transaction: dict[str, Any]) -> dict[str, Any]:
        """Predict fraud for a single transaction.

        Parameters
        ----------
        transaction : dict
            Must contain all features listed in FEATURES (in any order).

        Returns
        -------
        dict with:
            prediction    (int)   — 1 = FRAUD, 0 = NORMAL
            anomaly_score (float) — decision function score
            is_fraud      (bool)  — True if prediction == 1
            label         (str)   — "FRAUD" or "NORMAL"
        """
        check = self.validate(transaction)
        if not check["valid"]:
            raise ValueError(f"Invalid transaction: {check['errors']}")

        X = pd.DataFrame(
            {feat: [transaction[feat]] for feat in FEATURES},
            columns=FEATURES,
        )
        X_scaled = pd.DataFrame(
            FraudPredictionService._scaler.transform(X),
            columns=FEATURES,
        )

        raw_pred = FraudPredictionService._model.predict(X_scaled)[0]
        score    = float(FraudPredictionService._model.decision_function(X_scaled)[0])
        pred     = 1 if raw_pred == -1 else 0

        return {
            "prediction":    pred,
            "anomaly_score": round(score, 6),
            "is_fraud":      bool(pred == 1),
            "label":         "FRAUD" if pred == 1 else "NORMAL",
        }

    def predict_batch(self, transactions: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Predict fraud for a batch of transactions.

        Parameters
        ----------
        transactions : list of dict

        Returns
        -------
        List of result dicts (same order as input).
        """
        if not transactions:
            return []

        rows = []
        for i, txn in enumerate(transactions):
            check = self.validate(txn)
            if not check["valid"]:
                raise ValueError(f"Invalid transaction at index {i}: {check['errors']}")
            rows.append([txn[feat] for feat in FEATURES])

        X = pd.DataFrame(rows, columns=FEATURES)
        X_scaled = pd.DataFrame(
            FraudPredictionService._scaler.transform(X),
            columns=FEATURES,
        )

        raw_preds = FraudPredictionService._model.predict(X_scaled)
        scores    = FraudPredictionService._model.decision_function(X_scaled)

        return [
            {
                "prediction":    1 if raw == -1 else 0,
                "anomaly_score": round(float(s), 6),
                "is_fraud":      bool(raw == -1),
                "label":         "FRAUD" if raw == -1 else "NORMAL",
            }
            for raw, s in zip(raw_preds, scores)
        ]

    def model_info(self) -> dict:
        """Return model metadata for logging and monitoring."""
        return {
            "model_version":      self._metadata.get("model_version", "1.0.0"),
            "model_name":         self._metadata.get("model_name", "Isolation Forest"),
            "algorithm":          self._metadata.get("algorithm", "IsolationForest"),
            "training_date":      self._metadata.get("training_date", "2026-07-06"),
            "recall":             self._metadata.get("evaluation_metrics", {}).get("recall"),
            "throughput_per_sec": self._metadata.get("performance", {}).get("throughput_per_second"),
            "features":           FEATURES,
            "feature_count":      len(FEATURES),
        }

    def benchmark(self, n: int = 100) -> dict[str, float]:
        """Measure inference latency.

        Parameters
        ----------
        n : int — number of single-prediction repetitions

        Returns
        -------
        dict with latency_ms and throughput_per_sec
        """
        sample = {feat: 1.0 for feat in FEATURES}
        sample.update({
            "step": 1, "amount": 5000.0,
            "oldbalanceOrg": 20000.0, "newbalanceOrig": 15000.0,
        })

        t0 = time.perf_counter()
        for _ in range(n):
            self.predict(sample)
        total_ms = (time.perf_counter() - t0) * 1000
        avg_ms   = total_ms / n

        return {
            "n_predictions":     n,
            "total_ms":          round(total_ms, 4),
            "avg_latency_ms":    round(avg_ms, 6),
            "throughput_per_sec": round(1000 / avg_ms if avg_ms > 0 else 0, 2),
        }


# ---------------------------------------------------------------------------
# CLI Entry Point — standalone test
# ---------------------------------------------------------------------------
def main() -> None:
    logger.info("Initializing FraudPredictionService ...")
    service = FraudPredictionService()

    info = service.model_info()
    logger.info("Model: %s v%s", info["model_name"], info["model_version"])
    logger.info("Training date: %s", info["training_date"])
    logger.info("Recall: %s", info["recall"])

    # Test predictions
    normal_txn = {
        "step": 1, "amount": 9839.64,
        "oldbalanceOrg": 170136.0, "newbalanceOrig": 160296.36,
        "oldbalanceDest": 0.0, "newbalanceDest": 0.0,
        "type_CASH_OUT": 0, "type_DEBIT": 0, "type_PAYMENT": 1, "type_TRANSFER": 0,
    }
    fraud_txn = {
        "step": 233, "amount": 215310.30,
        "oldbalanceOrg": 215310.30, "newbalanceOrig": 0.0,
        "oldbalanceDest": 0.0, "newbalanceDest": 0.0,
        "type_CASH_OUT": 1, "type_DEBIT": 0, "type_PAYMENT": 0, "type_TRANSFER": 0,
    }

    logger.info("Normal transaction: %s", service.predict(normal_txn))
    logger.info("Fraud transaction:  %s", service.predict(fraud_txn))

    # Batch test
    batch = [normal_txn] * 500 + [fraud_txn] * 500
    t0 = time.perf_counter()
    results = service.predict_batch(batch)
    elapsed = (time.perf_counter() - t0) * 1000
    fraud_cnt = sum(1 for r in results if r["is_fraud"])
    logger.info("Batch 1000: %d fraud detected in %.2f ms", fraud_cnt, elapsed)

    # Benchmark
    bench = service.benchmark(n=100)
    logger.info("Benchmark: avg %.4f ms/txn | %.2f txn/sec", bench["avg_latency_ms"], bench["throughput_per_sec"])

    logger.info("✅ FraudPredictionService validation complete.")


if __name__ == "__main__":
    main()
