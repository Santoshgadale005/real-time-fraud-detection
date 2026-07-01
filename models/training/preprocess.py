"""Data Preprocessing Pipeline for the Fraud Detection Model.

This script loads the historical PaySim dataset, performs data cleaning,
one-hot encoding of categorical attributes, scaling of numerical attributes,
and exports the split train/test sets and fitted standard scaler.
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("fraud-preprocessing")

# ---------------------------------------------------------------------------
# Helper Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT_PATH = PROJECT_ROOT / "data/historical/paysim.csv"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data/processed"
DEFAULT_SCALER_PATH = PROJECT_ROOT / "models/scaler.pkl"


def load_data(filepath: str | Path) -> pd.DataFrame:
    """Load dataset from the CSV file."""
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Input dataset not found at {path}")
    logger.info("Loading dataset from %s ...", path)
    df = pd.read_csv(path)
    logger.info("Dataset loaded successfully. Shape: %s", df.shape)
    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Perform initial cleaning: remove duplicates and drop identifier columns."""
    df_clean = df.copy()

    # Check for duplicates
    duplicates = df_clean.duplicated().sum()
    if duplicates > 0:
        logger.info("Found %d duplicate records. Removing them ...", duplicates)
        df_clean.drop_duplicates(inplace=True)
    else:
        logger.info("No duplicate records found.")

    # Check for missing values
    missing = df_clean.isnull().sum().sum()
    logger.info("Total missing values across all columns: %d", missing)

    # Remove irrelevant identifier columns
    cols_to_drop = ["nameOrig", "nameDest"]
    # Also drop 'isFlaggedFraud' if present, as it is a rule-based column that we don't want to leak
    if "isFlaggedFraud" in df_clean.columns:
        cols_to_drop.append("isFlaggedFraud")

    existing_drops = [c for c in cols_to_drop if c in df_clean.columns]
    logger.info("Removing identifier/unused columns: %s", existing_drops)
    df_clean.drop(columns=existing_drops, inplace=True)

    return df_clean


def encode_features(df: pd.DataFrame) -> pd.DataFrame:
    """One-hot encode categorical variables."""
    logger.info("Encoding categorical transaction 'type' column ...")
    # Cast encoding to integer to prevent bool outputs in pandas 2.0+
    df_encoded = pd.get_dummies(df, columns=["type"], drop_first=True, dtype=int)
    logger.info("Encoded columns created: %s", [col for col in df_encoded.columns if "type_" in col])
    return df_encoded


def scale_features(X: pd.DataFrame, scaler_save_path: str | Path) -> tuple[np.ndarray, StandardScaler]:
    """Fit a standard scaler on the feature matrix X, scale it, and save the scaler."""
    logger.info("Scaling features using StandardScaler ...")
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    scaler_path = Path(scaler_save_path)
    scaler_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Saving fitted scaler to %s ...", scaler_path)
    joblib.dump(scaler, scaler_path)

    return X_scaled, scaler


def split_data(
    X: np.ndarray, y: pd.Series
) -> tuple[np.ndarray, np.ndarray, pd.Series, pd.Series]:
    """Split dataset into training (80%) and testing (20%) sets."""
    logger.info("Splitting dataset into 80%% training and 20%% testing sets (random_state=42) ...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    logger.info("Training size: %s | Testing size: %s", X_train.shape, X_test.shape)
    return X_train, X_test, y_train, y_test


def save_processed_data(
    X_train: np.ndarray,
    X_test: np.ndarray,
    y_train: pd.Series,
    y_test: pd.Series,
    feature_names: list[str],
    output_dir: str | Path,
) -> None:
    """Save the processed arrays and target series to CSV files in output_dir."""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Saving processed datasets to %s ...", out_dir)

    # Convert features back to DataFrame to preserve feature column names
    pd.DataFrame(X_train, columns=feature_names).to_csv(out_dir / "X_train.csv", index=False)
    pd.DataFrame(X_test, columns=feature_names).to_csv(out_dir / "X_test.csv", index=False)
    y_train.to_csv(out_dir / "y_train.csv", index=False)
    y_test.to_csv(out_dir / "y_test.csv", index=False)

    logger.info("All datasets successfully exported.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Preprocess historical PaySim transactions.")
    parser.add_argument(
        "--input",
        default=str(DEFAULT_INPUT_PATH),
        help="Path to the raw PaySim CSV file.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory to save preprocessed files.",
    )
    parser.add_argument(
        "--scaler-path",
        default=str(DEFAULT_SCALER_PATH),
        help="Path to save the fitted joblib scaler.",
    )
    args = parser.parse_args()

    # Run pipeline
    df = load_data(args.input)
    df_clean = clean_data(df)
    df_encoded = encode_features(df_clean)

    # Separate features and target
    X = df_encoded.drop(columns=["isFraud"])
    y = df_encoded["isFraud"]

    # Log class distribution
    class_counts = y.value_counts()
    imbalance_ratio = class_counts.get(1, 0) / len(y) * 100
    logger.info(
        "Class distribution: Non-Fraud=%d, Fraud=%d (%.4f%% Fraud)",
        class_counts.get(0, 0),
        class_counts.get(1, 0),
        imbalance_ratio,
    )

    X_scaled, _scaler = scale_features(X, args.scaler_path)
    X_train, X_test, y_train, y_test = split_data(X_scaled, y)

    save_processed_data(
        X_train,
        X_test,
        y_train,
        y_test,
        feature_names=list(X.columns),
        output_dir=args.output_dir,
    )

    logger.info("🎉 Preprocessing pipeline complete!")


if __name__ == "__main__":
    main()
