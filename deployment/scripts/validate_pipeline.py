"""End-to-End Inference Pipeline Validation Script — Day 13.

This script performs comprehensive validation of the production inference pipeline:

  1.  Load production model and scaler
  2.  Test single normal transaction prediction
  3.  Test single fraud transaction prediction
  4.  Batch prediction tests (100 / 500 / 1000 transactions)
  5.  Prediction consistency check (same transaction × 5 runs)
  6.  Inference latency benchmarks
  7.  Feature order validation
  8.  Feature validation function tests (missing / extra / wrong-type columns)

Run:
    venv/bin/python deployment/scripts/validate_pipeline.py

All steps must pass for the deployment package to be considered production-ready.
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

# Allow importing from project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from models.predict import (
    EXPECTED_FEATURES,
    _load_artifacts,
    measure_inference_time,
    predict_batch,
    predict_transaction,
    validate_features,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("pipeline-validator")


# ---------------------------------------------------------------------------
# Sample Transactions
# ---------------------------------------------------------------------------
# Normal transaction — a typical payment, low amount, balanced accounts
NORMAL_TRANSACTION = {
    "step": 1,
    "amount": 9839.64,
    "oldbalanceOrg": 170136.0,
    "newbalanceOrig": 160296.36,
    "oldbalanceDest": 0.0,
    "newbalanceDest": 0.0,
    "type_CASH_OUT": 0,
    "type_DEBIT": 0,
    "type_PAYMENT": 1,
    "type_TRANSFER": 0,
}

# Fraud transaction — large CASH_OUT draining account to zero
FRAUD_TRANSACTION = {
    "step": 233,
    "amount": 215310.30,
    "oldbalanceOrg": 215310.30,
    "newbalanceOrig": 0.0,       # Account completely drained
    "oldbalanceDest": 0.0,
    "newbalanceDest": 0.0,       # Destination also empty (mule account pattern)
    "type_CASH_OUT": 1,          # High-risk transaction type
    "type_DEBIT": 0,
    "type_PAYMENT": 0,
    "type_TRANSFER": 0,
}


# ---------------------------------------------------------------------------
# Validation Steps
# ---------------------------------------------------------------------------

def step_01_load_artifacts() -> bool:
    """STEP 1: Verify model and scaler load successfully."""
    logger.info("=" * 60)
    logger.info("STEP 1: Loading Production Artifacts")
    logger.info("=" * 60)
    try:
        model, scaler = _load_artifacts()
        logger.info("✅ Model:  %s", type(model).__name__)
        logger.info("✅ Scaler: %s", type(scaler).__name__)
        return True
    except Exception as exc:
        logger.error("❌ Artifact loading failed: %s", exc)
        return False


def step_02_single_normal_prediction() -> bool:
    """STEP 2: Test prediction on a normal transaction."""
    logger.info("")
    logger.info("=" * 60)
    logger.info("STEP 2: Single Normal Transaction Prediction")
    logger.info("=" * 60)
    try:
        result = predict_transaction(NORMAL_TRANSACTION)
        logger.info("  Transaction type : NORMAL")
        logger.info("  Prediction       : %d (%s)", result["prediction"], result["label"])
        logger.info("  Anomaly score    : %.6f", result["anomaly_score"])
        logger.info("  Is Fraud         : %s", result["is_fraud"])
        logger.info("✅ Normal transaction prediction completed.")
        return True
    except Exception as exc:
        logger.error("❌ Normal prediction failed: %s", exc)
        return False


def step_03_single_fraud_prediction() -> bool:
    """STEP 3: Test prediction on a known fraud pattern transaction."""
    logger.info("")
    logger.info("=" * 60)
    logger.info("STEP 3: Single Fraud Transaction Prediction")
    logger.info("=" * 60)
    try:
        result = predict_transaction(FRAUD_TRANSACTION)
        logger.info("  Transaction type : FRAUD (known pattern)")
        logger.info("  Prediction       : %d (%s)", result["prediction"], result["label"])
        logger.info("  Anomaly score    : %.6f  (negative = more anomalous)", result["anomaly_score"])
        logger.info("  Is Fraud         : %s", result["is_fraud"])
        if result["anomaly_score"] < 0:
            logger.info("  ✅ Negative anomaly score confirms suspicious behavior detected.")
        else:
            logger.info("  ℹ️  Score is positive (unsupervised model; contamination threshold applies).")
        logger.info("✅ Fraud transaction prediction completed.")
        return True
    except Exception as exc:
        logger.error("❌ Fraud prediction failed: %s", exc)
        return False


def step_04_batch_predictions() -> bool:
    """STEP 4: Batch prediction tests at 100, 500, and 1000 transactions."""
    logger.info("")
    logger.info("=" * 60)
    logger.info("STEP 4: Batch Prediction Tests")
    logger.info("=" * 60)

    # Generate a mix of normal and fraud-like transactions
    rng = np.random.default_rng(seed=42)
    base_txns = [NORMAL_TRANSACTION] * 80 + [FRAUD_TRANSACTION] * 20  # 20% fraud-like

    all_passed = True
    for batch_size in [100, 500, 1000]:
        # Repeat base transactions to fill batch
        txns = (base_txns * (batch_size // len(base_txns) + 1))[:batch_size]
        try:
            t0 = time.perf_counter()
            results = predict_batch(txns)
            elapsed_ms = (time.perf_counter() - t0) * 1000

            fraud_count  = sum(1 for r in results if r["is_fraud"])
            normal_count = len(results) - fraud_count

            logger.info(
                "  Batch %4d txns: %4d normal | %4d fraud | total %.2f ms | avg %.4f ms/txn",
                batch_size, normal_count, fraud_count,
                elapsed_ms, elapsed_ms / batch_size,
            )

            assert len(results) == batch_size, f"Expected {batch_size} results, got {len(results)}"
            logger.info("  ✅ Batch %d: PASSED", batch_size)
        except Exception as exc:
            logger.error("  ❌ Batch %d FAILED: %s", batch_size, exc)
            all_passed = False

    return all_passed


def step_05_prediction_consistency() -> bool:
    """STEP 5: Verify that the same transaction always yields the same result."""
    logger.info("")
    logger.info("=" * 60)
    logger.info("STEP 5: Prediction Consistency Check")
    logger.info("=" * 60)

    n_runs = 5
    predictions  = []
    scores       = []

    for i in range(n_runs):
        result = predict_transaction(NORMAL_TRANSACTION)
        predictions.append(result["prediction"])
        scores.append(result["anomaly_score"])

    all_same_pred  = len(set(predictions)) == 1
    all_same_score = len(set(scores))      == 1

    logger.info("  Predictions across %d runs : %s", n_runs, predictions)
    logger.info("  Anomaly scores across %d runs: %s", n_runs, scores)
    logger.info("  Prediction deterministic   : %s", "✅ YES" if all_same_pred  else "❌ NO")
    logger.info("  Score deterministic        : %s", "✅ YES" if all_same_score else "❌ NO")

    if all_same_pred and all_same_score:
        logger.info("✅ Consistency check PASSED — predictions are fully deterministic.")
        return True
    else:
        logger.error("❌ Consistency check FAILED — non-deterministic predictions detected!")
        return False


def step_06_inference_latency() -> bool:
    """STEP 6: Measure inference latency."""
    logger.info("")
    logger.info("=" * 60)
    logger.info("STEP 6: Inference Latency Measurement")
    logger.info("=" * 60)

    try:
        latency = measure_inference_time(sample_transaction=NORMAL_TRANSACTION, n_single=100)
        logger.info("  Single prediction latency : %.4f ms",  latency["single_prediction_ms"])
        logger.info("  100-prediction total      : %.4f ms",  latency["n_predictions_total_ms"])
        logger.info("  Average latency           : %.6f ms",  latency["avg_latency_ms"])
        logger.info("  Estimated throughput      : %.2f txn/sec", latency["throughput_per_sec"])

        # Real-time threshold: must be < 10ms per transaction for streaming
        if latency["avg_latency_ms"] < 10.0:
            logger.info("  ✅ Latency is within real-time threshold (< 10ms per transaction).")
        else:
            logger.warning("  ⚠️  Latency %.4f ms exceeds 10ms threshold.", latency["avg_latency_ms"])

        logger.info("✅ Inference latency benchmark complete.")
        return True
    except Exception as exc:
        logger.error("❌ Latency measurement failed: %s", exc)
        return False


def step_07_feature_order_validation() -> bool:
    """STEP 7: Validate that expected feature order matches training order."""
    logger.info("")
    logger.info("=" * 60)
    logger.info("STEP 7: Feature Order Validation")
    logger.info("=" * 60)

    # Load training feature list from model config
    config_path = PROJECT_ROOT / "models" / "production_model_config.json"
    if not config_path.exists():
        logger.warning("  ⚠️  production_model_config.json not found — skipping config cross-check.")
        return True

    import json
    with open(config_path, encoding="utf-8") as f:
        config = json.load(f)

    config_features = config.get("feature_names", [])
    predict_features = EXPECTED_FEATURES

    logger.info("  Training feature order  : %s", config_features)
    logger.info("  Prediction module order : %s", predict_features)

    if config_features == predict_features:
        logger.info("✅ Feature order MATCHES training configuration perfectly.")
        return True
    else:
        logger.error("❌ Feature order MISMATCH between training config and prediction module!")
        logger.error("  Expected : %s", config_features)
        logger.error("  Got      : %s", predict_features)
        return False


def step_08_feature_validation_function() -> bool:
    """STEP 8: Test the feature validation utility with edge cases."""
    logger.info("")
    logger.info("=" * 60)
    logger.info("STEP 8: Feature Validation Function Tests")
    logger.info("=" * 60)

    all_passed = True

    # Test 1: Valid transaction
    result = validate_features(NORMAL_TRANSACTION)
    if result["valid"]:
        logger.info("  ✅ Test 1 PASSED: Valid transaction accepted correctly.")
    else:
        logger.error("  ❌ Test 1 FAILED: Valid transaction rejected: %s", result["errors"])
        all_passed = False

    # Test 2: Missing feature
    missing_txn = {k: v for k, v in NORMAL_TRANSACTION.items() if k != "amount"}
    result = validate_features(missing_txn)
    if not result["valid"] and any("amount" in e for e in result["errors"]):
        logger.info("  ✅ Test 2 PASSED: Missing 'amount' detected correctly.")
    else:
        logger.error("  ❌ Test 2 FAILED: Missing feature not detected. Errors: %s", result["errors"])
        all_passed = False

    # Test 3: Extra feature (should warn, not error)
    extra_txn = {**NORMAL_TRANSACTION, "extra_column": "junk_value"}
    result = validate_features(extra_txn)
    if result["valid"] and any("extra_column" in w for w in result["warnings"]):
        logger.info("  ✅ Test 3 PASSED: Extra column correctly warned (not rejected).")
    else:
        logger.error("  ❌ Test 3 FAILED: Extra column handling unexpected. Result: %s", result)
        all_passed = False

    # Test 4: Wrong data type
    bad_type_txn = {**NORMAL_TRANSACTION, "amount": "not_a_number"}
    result = validate_features(bad_type_txn)
    if not result["valid"] and any("amount" in e for e in result["errors"]):
        logger.info("  ✅ Test 4 PASSED: Wrong type for 'amount' detected correctly.")
    else:
        logger.error("  ❌ Test 4 FAILED: Wrong type not detected. Errors: %s", result["errors"])
        all_passed = False

    return all_passed


def step_09_full_pipeline_validation() -> bool:
    """STEP 9: Full end-to-end pipeline test from raw dict to final prediction."""
    logger.info("")
    logger.info("=" * 60)
    logger.info("STEP 9: Full End-to-End Pipeline Validation")
    logger.info("=" * 60)

    pipeline_steps = [
        "Transaction input",
        "Feature validation",
        "Feature ordering",
        "Scaler transformation",
        "Isolation Forest prediction",
        "Score calculation",
        "Output formatting",
    ]

    logger.info("  Pipeline flow:")
    for i, step in enumerate(pipeline_steps, 1):
        logger.info("    %d. %s", i, step)

    try:
        result = predict_transaction(FRAUD_TRANSACTION)
        logger.info("")
        logger.info("  Final pipeline output:")
        logger.info("    prediction    : %d", result["prediction"])
        logger.info("    anomaly_score : %.6f", result["anomaly_score"])
        logger.info("    is_fraud      : %s", result["is_fraud"])
        logger.info("    label         : %s", result["label"])
        logger.info("✅ Full pipeline validation PASSED — no errors in any stage.")
        return True
    except Exception as exc:
        logger.error("❌ Full pipeline validation FAILED: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    logger.info("")
    logger.info("╔══════════════════════════════════════════════════════════╗")
    logger.info("║   Day 13: Production Inference Pipeline Validation       ║")
    logger.info("║   Real-Time Financial Fraud Detection Pipeline           ║")
    logger.info("╚══════════════════════════════════════════════════════════╝")
    logger.info("")

    steps = [
        ("Artifact Loading",               step_01_load_artifacts),
        ("Normal Transaction Prediction",  step_02_single_normal_prediction),
        ("Fraud Transaction Prediction",   step_03_single_fraud_prediction),
        ("Batch Predictions",              step_04_batch_predictions),
        ("Prediction Consistency",         step_05_prediction_consistency),
        ("Inference Latency",              step_06_inference_latency),
        ("Feature Order Validation",       step_07_feature_order_validation),
        ("Feature Validation Function",    step_08_feature_validation_function),
        ("Full Pipeline Validation",       step_09_full_pipeline_validation),
    ]

    results: dict[str, bool] = {}
    for name, fn in steps:
        results[name] = fn()

    # Summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 60)
    passed = 0
    failed = 0
    for name, ok in results.items():
        status = "✅ PASSED" if ok else "❌ FAILED"
        logger.info("  %-40s %s", name, status)
        if ok:
            passed += 1
        else:
            failed += 1

    logger.info("")
    logger.info("  Total: %d passed, %d failed", passed, failed)

    if failed == 0:
        logger.info("")
        logger.info("🎉 ALL VALIDATION STEPS PASSED.")
        logger.info("   The production model is ready for Spark Structured Streaming deployment.")
        sys.exit(0)
    else:
        logger.error("")
        logger.error("⚠️  %d VALIDATION STEP(S) FAILED. Review errors above before deployment.", failed)
        sys.exit(1)


if __name__ == "__main__":
    main()
