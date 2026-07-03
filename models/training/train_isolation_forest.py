"""Train an Isolation Forest model for anomaly-based fraud detection.

This script loads the preprocessed numerical feature datasets, trains an
unsupervised Isolation Forest model, converts anomaly predictions to standard
fraud labels, saves predictions, and serializes the model and its metadata.
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import joblib
import pandas as pd
from sklearn.ensemble import IsolationForest

# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("fraud-training")

# ---------------------------------------------------------------------------
# Helper Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TRAIN_FEATURES_PATH = PROJECT_ROOT / "data/processed/X_train.csv"
DEFAULT_TEST_FEATURES_PATH = PROJECT_ROOT / "data/processed/X_test.csv"
DEFAULT_TEST_TARGET_PATH = PROJECT_ROOT / "data/processed/y_test.csv"
DEFAULT_MODEL_SAVE_PATH = PROJECT_ROOT / "models/isolation_forest.pkl"
DEFAULT_METADATA_SAVE_PATH = PROJECT_ROOT / "models/model_info.json"
DEFAULT_RESULTS_SAVE_PATH = PROJECT_ROOT / "data/results/fraud_predictions.csv"


def load_datasets(
    train_feat_path: str | Path,
    test_feat_path: str | Path,
    test_target_path: str | Path,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series]:
    """Load the preprocessed feature and target CSV files."""
    logger.info("Loading preprocessed training and test datasets ...")
    X_train = pd.read_csv(Path(train_feat_path))
    X_test = pd.read_csv(Path(test_feat_path))
    # Load target series (squeeze=True is deprecated in modern pandas, so read_csv + squeeze manually)
    y_test_df = pd.read_csv(Path(test_target_path))
    # Ensure y_test is a single Series
    y_test = y_test_df.iloc[:, 0] if not y_test_df.empty else pd.Series(dtype=int)

    logger.info(
        "Datasets loaded: X_train=%s, X_test=%s, y_test=%s",
        X_train.shape,
        X_test.shape,
        y_test.shape,
    )
    return X_train, X_test, y_test


def train_model(
    X_train: pd.DataFrame,
    contamination: float,
    n_estimators: int,
    random_state: int,
) -> IsolationForest:
    """Instantiate and train the Isolation Forest model on X_train."""
    logger.info("Initializing Isolation Forest anomaly detection model ...")
    logger.info("  Contamination : %f", contamination)
    logger.info("  N Estimators  : %d", n_estimators)
    logger.info("  Random State  : %d", random_state)

    model = IsolationForest(
        contamination=contamination,
        n_estimators=n_estimators,
        random_state=random_state,
        n_jobs=-1,  # use all available CPU cores
    )

    logger.info("Fitting Isolation Forest model on training set (normal behavior baseline) ...")
    model.fit(X_train)
    logger.info("Model fitting complete!")
    return model


def generate_predictions(
    model: IsolationForest, X_test: pd.DataFrame
) -> tuple[list[int], list[float]]:
    """Predict anomalies on the test set and calculate decision/anomaly scores."""
    logger.info("Generating predictions on the test dataset ...")
    # Predict returns: 1 for normal, -1 for anomaly
    raw_predictions = model.predict(X_test)

    # Convert predictions to classification format: 1 for anomaly (fraud), 0 for normal
    converted_predictions = [1 if p == -1 else 0 for p in raw_predictions]

    # Calculate anomaly decision score (negative score = anomaly, positive = normal)
    logger.info("Calculating anomaly decision scores ...")
    decision_scores = model.decision_function(X_test)

    return converted_predictions, list(decision_scores)


def save_results(
    X_test: pd.DataFrame,
    y_test: pd.Series,
    predictions: list[int],
    scores: list[float],
    save_path: str | Path,
) -> pd.DataFrame:
    """Combine test features, target, predictions, and anomaly scores, then save as CSV."""
    results_path = Path(save_path)
    results_path.parent.mkdir(parents=True, exist_ok=True)

    results = X_test.copy()
    results["Actual"] = y_test.values
    results["Prediction"] = predictions
    results["Score"] = scores

    logger.info("Saving prediction results to %s ...", results_path)
    results.to_csv(results_path, index=False)
    logger.info("Results successfully saved.")
    return results


def save_model_artifacts(
    model: IsolationForest,
    model_path: str | Path,
    metadata_path: str | Path,
    contamination: float,
    n_estimators: int,
    random_state: int,
    X_train: pd.DataFrame,
    X_test: pd.DataFrame,
    results: pd.DataFrame,
) -> None:
    """Serialize the trained model using joblib and export metadata to JSON."""
    m_path = Path(model_path)
    m_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Serializing trained model binary to %s ...", m_path)
    joblib.dump(model, m_path)
    logger.info("Model serialized successfully.")

    metadata = {
        "model_name": "Isolation Forest Anomaly Detector",
        "algorithm": "sklearn.ensemble.IsolationForest",
        "training_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "parameters": {
            "contamination": contamination,
            "n_estimators": n_estimators,
            "random_state": random_state,
        },
        "dataset_version": "PaySim Subset Day 8 Preprocessed",
        "training_rows": int(len(X_train)),
        "test_rows": int(len(X_test)),
        "feature_count": int(X_train.shape[1]),
        "feature_names": list(X_train.columns),
        "prediction_summary": {
            "predicted_normal": int((results["Prediction"] == 0).sum()),
            "predicted_fraud": int((results["Prediction"] == 1).sum()),
            "actual_normal": int((results["Actual"] == 0).sum()),
            "actual_fraud": int((results["Actual"] == 1).sum()),
        },
    }

    meta_path = Path(metadata_path)
    logger.info("Saving model metadata to %s ...", meta_path)
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=4)
    logger.info("Metadata saved successfully.")


def verify_model(model_path: str | Path, X_test: pd.DataFrame) -> None:
    """Verify that the serialized model can be successfully reloaded and predicts."""
    logger.info("Performing model verification: reloading model ...")
    loaded_model = joblib.load(Path(model_path))
    logger.info("Model successfully loaded from binary.")

    # Run quick test prediction
    test_preds = loaded_model.predict(X_test.head(5))
    logger.info("Test reload verification success. Sample predictions: %s", test_preds)


def main() -> None:
    parser = argparse.ArgumentParser(description="Train Isolation Forest anomaly detection model.")
    parser.add_argument(
        "--train-features",
        default=str(DEFAULT_TRAIN_FEATURES_PATH),
        help="Path to the training features CSV.",
    )
    parser.add_argument(
        "--test-features",
        default=str(DEFAULT_TEST_FEATURES_PATH),
        help="Path to the test features CSV.",
    )
    parser.add_argument(
        "--test-target",
        default=str(DEFAULT_TEST_TARGET_PATH),
        help="Path to the test target CSV.",
    )
    parser.add_argument(
        "--model-path",
        default=str(DEFAULT_MODEL_SAVE_PATH),
        help="Path to save the trained model pickle file.",
    )
    parser.add_argument(
        "--metadata-path",
        default=str(DEFAULT_METADATA_SAVE_PATH),
        help="Path to save model metadata JSON.",
    )
    parser.add_argument(
        "--results-path",
        default=str(DEFAULT_RESULTS_SAVE_PATH),
        help="Path to save prediction results CSV.",
    )
    parser.add_argument(
        "--contamination",
        type=float,
        default=0.001,
        help="The expected ratio of anomalies (fraud) in the dataset.",
    )
    parser.add_argument(
        "--n-estimators",
        type=int,
        default=100,
        help="Number of isolation trees to build in the forest.",
    )
    parser.add_argument(
        "--random-state",
        type=int,
        default=42,
        help="Random state for reproducibility.",
    )
    args = parser.parse_args()

    # Load preprocessed datasets
    X_train, X_test, y_test = load_datasets(
        args.train_features, args.test_features, args.test_target
    )

    # Train Isolation Forest
    model = train_model(
        X_train,
        contamination=args.contamination,
        n_estimators=args.n_estimators,
        random_state=args.random_state,
    )

    # Predict anomalies and compute scores
    predictions, scores = generate_predictions(model, X_test)

    # Save results
    results = save_results(X_test, y_test, predictions, scores, args.results_path)

    # Print summary statistics of predictions
    pred_counts = results["Prediction"].value_counts()
    anomaly_rate = pred_counts.get(1, 0) / len(results) * 100
    actual_counts = results["Actual"].value_counts()
    logger.info(
        "Prediction statistics: Anomalies (Fraud)=%d, Normal=%d (%.4f%% Anomalies)",
        pred_counts.get(1, 0),
        pred_counts.get(0, 0),
        anomaly_rate,
    )
    logger.info(
        "Actual class statistics in test set: Fraud=%d, Normal=%d",
        actual_counts.get(1, 0),
        actual_counts.get(0, 0),
    )

    # Save trained model binary and metadata
    save_model_artifacts(
        model,
        args.model_path,
        args.metadata_path,
        contamination=args.contamination,
        n_estimators=args.n_estimators,
        random_state=args.random_state,
        X_train=X_train,
        X_test=X_test,
        results=results,
    )

    # Reload and verify
    verify_model(args.model_path, X_test)

    logger.info("🎉 Day 9 model training pipeline complete!")


if __name__ == "__main__":
    main()
