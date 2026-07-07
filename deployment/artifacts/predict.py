"""Reusable Prediction Module for Real-Time Fraud Detection.

This module is the single entry point for all inference operations.
It loads the production Isolation Forest model and the fitted StandardScaler,
validates incoming transaction data, and exposes a clean API for:

  - Single-transaction prediction  (used by Spark Structured Streaming)
  - Batch prediction               (used for bulk testing)
  - Inference latency measurement  (used for production benchmarking)

Usage
-----
    from models.predict import predict_transaction, predict_batch

    result = predict_transaction({
        "step": 1, "amount": 9839.64,
        "oldbalanceOrg": 170136.0, "newbalanceOrig": 160296.36,
        "oldbalanceDest": 0.0, "newbalanceDest": 0.0,
        "type_CASH_OUT": 0, "type_DEBIT": 0,
        "type_PAYMENT": 0, "type_TRANSFER": 1,
    })
    # → {"prediction": 0, "anomaly_score": 0.043, "is_fraud": False, "label": "NORMAL"}
"""

from __future__ import annotations

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
logger = logging.getLogger("fraud-predictor")

# ---------------------------------------------------------------------------
# Artifact Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODEL_PATH   = PROJECT_ROOT / "models" / "production_model.pkl"
SCALER_PATH  = PROJECT_ROOT / "models" / "scaler.pkl"

# ---------------------------------------------------------------------------
# Expected feature schema (must match training preprocessing exactly)
# ---------------------------------------------------------------------------
EXPECTED_FEATURES: list[str] = [
    "step",
    "amount",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
    "type_CASH_OUT",
    "type_DEBIT",
    "type_PAYMENT",
    "type_TRANSFER",
]

EXPECTED_DTYPES: dict[str, type] = {
    "step":           (int, float, np.integer, np.floating),
    "amount":         (int, float, np.integer, np.floating),
    "oldbalanceOrg":  (int, float, np.integer, np.floating),
    "newbalanceOrig": (int, float, np.integer, np.floating),
    "oldbalanceDest": (int, float, np.integer, np.floating),
    "newbalanceDest": (int, float, np.integer, np.floating),
    "type_CASH_OUT":  (int, float, np.integer, np.floating),
    "type_DEBIT":     (int, float, np.integer, np.floating),
    "type_PAYMENT":   (int, float, np.integer, np.floating),
    "type_TRANSFER":  (int, float, np.integer, np.floating),
}


# ---------------------------------------------------------------------------
# Model Loading (lazy singleton)
# ---------------------------------------------------------------------------
_model  = None
_scaler = None


def _load_artifacts() -> tuple:
    """Load production model and scaler (once, then cached)."""
    global _model, _scaler
    if _model is None:
        if not MODEL_PATH.exists():
            raise FileNotFoundError(f"Production model not found: {MODEL_PATH}")
        logger.info("Loading production model from %s ...", MODEL_PATH)
        _model = joblib.load(MODEL_PATH)
        logger.info("✅ Model loaded successfully. Type: %s", type(_model).__name__)

    if _scaler is None:
        if not SCALER_PATH.exists():
            raise FileNotFoundError(f"Scaler not found: {SCALER_PATH}")
        logger.info("Loading scaler from %s ...", SCALER_PATH)
        _scaler = joblib.load(SCALER_PATH)
        logger.info("✅ Scaler loaded successfully. Type: %s", type(_scaler).__name__)

    return _model, _scaler


# ---------------------------------------------------------------------------
# Feature Validation (Step 11)
# ---------------------------------------------------------------------------
def validate_features(transaction: dict[str, Any]) -> dict[str, Any]:
    """Validate that a transaction dict contains the correct features.

    Parameters
    ----------
    transaction : dict
        Raw transaction key-value pairs.

    Returns
    -------
    dict with keys:
        "valid"   (bool)  — True if the transaction passes all checks
        "errors"  (list)  — List of validation error messages
        "warnings"(list)  — Non-fatal issues (e.g. extra columns ignored)
    """
    errors:   list[str] = []
    warnings: list[str] = []

    incoming = set(transaction.keys())
    expected = set(EXPECTED_FEATURES)

    # 1. Missing features
    missing = expected - incoming
    if missing:
        errors.append(f"Missing required features: {sorted(missing)}")

    # 2. Extra features (not fatal — they will be ignored, but we warn)
    extra = incoming - expected
    if extra:
        warnings.append(f"Extra features will be ignored: {sorted(extra)}")

    # 3. Type checks for present features
    for feat, allowed_types in EXPECTED_DTYPES.items():
        if feat in transaction:
            val = transaction[feat]
            if not isinstance(val, allowed_types):
                errors.append(
                    f"Feature '{feat}' has wrong type: "
                    f"expected numeric, got {type(val).__name__} (value={val!r})"
                )

    return {
        "valid":    len(errors) == 0,
        "errors":   errors,
        "warnings": warnings,
    }


# ---------------------------------------------------------------------------
# Core Prediction Functions
# ---------------------------------------------------------------------------
def predict_transaction(transaction: dict[str, Any]) -> dict[str, Any]:
    """Predict fraud for a single transaction.

    Parameters
    ----------
    transaction : dict
        A key-value mapping with exactly the fields listed in EXPECTED_FEATURES.
        Extra fields are ignored; missing fields raise a ValueError.

    Returns
    -------
    dict with keys:
        "prediction"    (int)   — 1 = fraud/anomaly, 0 = normal
        "anomaly_score" (float) — decision function score (more negative = more anomalous)
        "is_fraud"      (bool)  — True if prediction == 1
        "label"         (str)   — "FRAUD" or "NORMAL"
    """
    # Validate
    validation = validate_features(transaction)
    if not validation["valid"]:
        raise ValueError(
            f"Invalid transaction data: {validation['errors']}"
        )
    if validation["warnings"]:
        for w in validation["warnings"]:
            logger.warning("Feature warning: %s", w)

    model, scaler = _load_artifacts()

    # Build feature vector in exact training order (use DataFrame to preserve feature names)
    row = {feat: [transaction[feat]] for feat in EXPECTED_FEATURES}
    X = pd.DataFrame(row, columns=EXPECTED_FEATURES)

    # Apply same scaling used during training
    X_scaled = pd.DataFrame(
        scaler.transform(X), columns=EXPECTED_FEATURES
    )

    # Run Isolation Forest prediction
    raw_pred    = model.predict(X_scaled)[0]        # 1 = normal, -1 = anomaly
    score       = float(model.decision_function(X_scaled)[0])
    prediction  = 1 if raw_pred == -1 else 0        # convert to fraud=1, normal=0

    return {
        "prediction":    prediction,
        "anomaly_score": round(score, 6),
        "is_fraud":      bool(prediction == 1),
        "label":         "FRAUD" if prediction == 1 else "NORMAL",
    }


def predict_batch(transactions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Predict fraud for a list of transactions.

    Parameters
    ----------
    transactions : list of dict
        Each element follows the same schema as predict_transaction().

    Returns
    -------
    List of result dicts, one per input transaction (same order).
    """
    if not transactions:
        return []

    model, scaler = _load_artifacts()

    # Validate all rows
    rows = []
    for i, txn in enumerate(transactions):
        validation = validate_features(txn)
        if not validation["valid"]:
            raise ValueError(
                f"Invalid transaction at index {i}: {validation['errors']}"
            )
        rows.append([txn[feat] for feat in EXPECTED_FEATURES])

    X = pd.DataFrame(rows, columns=EXPECTED_FEATURES)
    X_scaled = pd.DataFrame(
        scaler.transform(X), columns=EXPECTED_FEATURES
    )

    raw_preds = model.predict(X_scaled)           # array of 1/-1
    scores    = model.decision_function(X_scaled)  # array of floats

    results = []
    for raw, score in zip(raw_preds, scores):
        pred = 1 if raw == -1 else 0
        results.append({
            "prediction":    pred,
            "anomaly_score": round(float(score), 6),
            "is_fraud":      bool(pred == 1),
            "label":         "FRAUD" if pred == 1 else "NORMAL",
        })

    return results


# ---------------------------------------------------------------------------
# Inference Latency Measurement (Step 9)
# ---------------------------------------------------------------------------
def measure_inference_time(
    sample_transaction: dict[str, Any] | None = None,
    n_single: int = 100,
) -> dict[str, float]:
    """Measure inference latency for single-transaction predictions.

    Parameters
    ----------
    sample_transaction : dict, optional
        Transaction to use for benchmarking. Defaults to a synthetic normal txn.
    n_single : int
        Number of single-prediction repetitions.

    Returns
    -------
    dict with keys:
        "single_prediction_ms"   — time for one prediction in ms
        "n_predictions_total_ms" — total time for n_single predictions in ms
        "avg_latency_ms"         — average latency per prediction
        "throughput_per_sec"     — estimated transactions per second
    """
    if sample_transaction is None:
        sample_transaction = {
            "step": 1, "amount": 5000.0,
            "oldbalanceOrg": 20000.0, "newbalanceOrig": 15000.0,
            "oldbalanceDest": 1000.0, "newbalanceDest": 6000.0,
            "type_CASH_OUT": 0, "type_DEBIT": 0,
            "type_PAYMENT": 1, "type_TRANSFER": 0,
        }

    # Single prediction
    t0 = time.perf_counter()
    predict_transaction(sample_transaction)
    single_ms = (time.perf_counter() - t0) * 1000

    # N predictions
    t1 = time.perf_counter()
    for _ in range(n_single):
        predict_transaction(sample_transaction)
    total_ms = (time.perf_counter() - t1) * 1000
    avg_ms   = total_ms / n_single

    throughput = 1000 / avg_ms if avg_ms > 0 else float("inf")

    return {
        "single_prediction_ms":   round(single_ms, 4),
        "n_predictions_total_ms": round(total_ms, 4),
        "avg_latency_ms":         round(avg_ms, 6),
        "throughput_per_sec":     round(throughput, 2),
    }
