"""Evaluate Isolation Forest fraud predictions.

This module evaluates the baseline predictions, writes model metrics,
saves a confusion matrix plot, and creates a Markdown evaluation summary.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
)

# Set non-interactive backend for matplotlib before importing pyplot
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

PROJECT_ROOT = Path(__file__).resolve().parents[2]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("fraud-evaluation")

DEFAULT_PREDICTIONS_PATH = PROJECT_ROOT / "data/results/fraud_predictions.csv"
DEFAULT_METRICS_PATH = PROJECT_ROOT / "data/results/model_metrics.csv"
DEFAULT_CONFUSION_MATRIX_PATH = PROJECT_ROOT / "data/results/confusion_matrix.png"
DEFAULT_SUMMARY_PATH = PROJECT_ROOT / "docs/evaluation_summary.md"


def load_prediction_results(predictions_path: str | Path) -> pd.DataFrame:
    """Load prediction results and validate the required columns."""
    path = Path(predictions_path)
    if not path.exists():
        raise FileNotFoundError(f"Prediction results not found at {path}")

    results = pd.read_csv(path)
    required_columns = {"Actual", "Prediction"}
    missing_columns = required_columns.difference(results.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns in prediction results: {missing_columns}")

    logger.info("Loaded prediction results from %s with shape %s", path, results.shape)
    return results


def calculate_metrics(y_true: pd.Series, y_pred: pd.Series) -> dict[str, float | int]:
    """Calculate core classification metrics and confusion matrix counts."""
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()
    metrics = {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "f1_score": float(f1_score(y_true, y_pred, zero_division=0)),
        "true_negatives": int(tn),
        "false_positives": int(fp),
        "false_negatives": int(fn),
        "true_positives": int(tp),
        "total_transactions": int(len(y_true)),
        "actual_frauds": int((y_true == 1).sum()),
        "predicted_frauds": int((y_pred == 1).sum()),
    }
    logger.info("Calculated evaluation metrics: %s", metrics)
    return metrics


def save_metrics(metrics: dict[str, float | int], metrics_path: str | Path) -> None:
    """Save evaluation metrics as a single-row CSV file."""
    path = Path(metrics_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{
        "Accuracy": metrics["accuracy"],
        "Precision": metrics["precision"],
        "Recall": metrics["recall"],
        "F1 Score": metrics["f1_score"],
    }]).to_csv(path, index=False)
    logger.info("Saved model metrics to %s", path)


def save_confusion_matrix_plot(
    y_true: pd.Series,
    y_pred: pd.Series,
    output_path: str | Path,
) -> list[list[int]]:
    """Save a labeled confusion matrix heatmap for review."""
    cm = confusion_matrix(y_true, y_pred, labels=[0, 1])
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    plt.figure(figsize=(7, 5))
    sns.heatmap(
        cm,
        annot=True,
        fmt="d",
        cmap="Blues",
        xticklabels=["Predicted Normal", "Predicted Fraud"],
        yticklabels=["Actual Normal", "Actual Fraud"],
    )
    plt.xlabel("Predicted")
    plt.ylabel("Actual")
    plt.title("Isolation Forest Confusion Matrix")
    plt.tight_layout()
    plt.savefig(path, dpi=160)
    plt.close()
    logger.info("Saved confusion matrix plot to %s", path)
    return cm.tolist()


def display_path(path: str | Path) -> str:
    """Return a repo-relative path for documentation when possible."""
    resolved = Path(path).resolve()
    try:
        return str(resolved.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(resolved)


def write_evaluation_summary(
    summary_path: str | Path,
    metrics: dict[str, float | int],
    classification_text: str,
    confusion_matrix_values: list[list[int]],
    prediction_counts: pd.Series,
    comparison_df: pd.DataFrame,
    paths: dict[str, str],
) -> None:
    """Write a Markdown summary report with metrics and initial observations."""
    path = Path(summary_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    # Format the crosstab and prediction counts as markdown tables/text
    pred_counts_text = prediction_counts.to_string()
    comparison_text = comparison_df.to_string()

    report = f"""# Model Evaluation Summary

**Project**: Real-Time Financial Fraud Detection Pipeline  
**Evaluation Date**: July 4, 2026  
**Model**: Isolation Forest Anomaly Detector  
**Dataset**: Day 9 prediction output from `data/results/fraud_predictions.csv`

## Model Parameters

| Parameter | Value |
|-----------|-------|
| Algorithm | `sklearn.ensemble.IsolationForest` |
| Contamination | `0.001` |
| Number of trees | `100` |
| Random state | `42` |

## Evaluation Metrics

| Metric | Value | Description |
|--------|-------|-------------|
| Accuracy | `{metrics["accuracy"]:.6f}` | Proportion of correct predictions overall. |
| Precision | `{metrics["precision"]:.6f}` | Proportion of predicted anomalies that were actual fraud. |
| Recall | `{metrics["recall"]:.6f}` | Proportion of actual fraud cases successfully detected. |
| F1 Score | `{metrics["f1_score"]:.6f}` | Harmonic mean of Precision and Recall. |

## Confusion Matrix

| Actual / Predicted | Normal | Fraud |
|--------------------|--------|-------|
| Actual Normal | `{metrics["true_negatives"]}` | `{metrics["false_positives"]}` |
| Actual Fraud | `{metrics["false_negatives"]}` | `{metrics["true_positives"]}` |

Raw matrix:
```text
{confusion_matrix_values}
```

## Classification Report

```text
{classification_text}
```

## Prediction Counts

How many anomalies were flagged by the model:
```text
{pred_counts_text}
```

## Actual vs Predicted Fraud Cross-Tabulation

```text
{comparison_text}
```

## Initial Observations

- **Accuracy is Misleading**: The baseline model achieves `{metrics["accuracy"] * 100:.2f}%` accuracy, but this is solely due to class imbalance (only `{metrics["actual_frauds"]}` frauds in `{metrics["total_transactions"]}` transactions).
- **Extremely Low Recall**: The model has a recall of `{metrics["recall"]:.6f}` (detecting only `{metrics["true_positives"]}` out of `{metrics["actual_frauds"]}` fraud cases). It missed `{metrics["false_negatives"]}` fraud events (False Negatives), representing high-risk financial leaks.
- **Moderate Precision**: The precision is `{metrics["precision"]:.6f}`, meaning only `{metrics["precision"] * 100:.2f}%` of the transactions flagged as anomalies were actual fraud cases.
- **Identified Weaknesses**: The baseline Isolation Forest setup (`contamination=0.001`) is too conservative. It flags only `{metrics["predicted_frauds"]}` total transactions as anomalies out of `{metrics["total_transactions"]}` test rows. We need to optimize contamination and threshold boundaries on Day 11.

## Generated Artifacts

- **Metrics CSV**: `{paths["metrics"]}`
- **Confusion Matrix Heatmap**: `{paths["confusion_matrix"]}`
"""
    path.write_text(report, encoding="utf-8")
    logger.info("Saved evaluation summary to %s", path)


def evaluate(args: argparse.Namespace) -> dict[str, float | int]:
    """Run the full evaluation workflow."""
    results = load_prediction_results(args.predictions_path)
    y_true = results["Actual"]
    y_pred = results["Prediction"]

    metrics = calculate_metrics(y_true, y_pred)
    classification_text = classification_report(y_true, y_pred, zero_division=0)
    prediction_counts = results["Prediction"].value_counts()
    comparison_df = pd.crosstab(results["Actual"], results["Prediction"])

    save_metrics(metrics, args.metrics_path)
    confusion_matrix_values = save_confusion_matrix_plot(
        y_true,
        y_pred,
        args.confusion_matrix_path,
    )

    write_evaluation_summary(
        args.summary_path,
        metrics,
        classification_text,
        confusion_matrix_values,
        prediction_counts,
        comparison_df,
        paths={
            "metrics": display_path(args.metrics_path),
            "confusion_matrix": display_path(args.confusion_matrix_path),
        },
    )

    print("Model Evaluation Metrics")
    print("------------------------")
    print(f"Accuracy : {metrics['accuracy']:.6f}")
    print(f"Precision: {metrics['precision']:.6f}")
    print(f"Recall   : {metrics['recall']:.6f}")
    print(f"F1 Score : {metrics['f1_score']:.6f}")
    print()
    print("Confusion Matrix:")
    print(confusion_matrix_values)
    print()
    print("Prediction Counts:")
    print(prediction_counts)
    print()
    print("Actual vs Predicted Cross-Tabulation:")
    print(comparison_df)
    print()
    print("Classification Report:")
    print(classification_text)
    return metrics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate Isolation Forest predictions.")
    parser.add_argument("--predictions-path", default=str(DEFAULT_PREDICTIONS_PATH))
    parser.add_argument("--metrics-path", default=str(DEFAULT_METRICS_PATH))
    parser.add_argument("--confusion-matrix-path", default=str(DEFAULT_CONFUSION_MATRIX_PATH))
    parser.add_argument("--summary-path", default=str(DEFAULT_SUMMARY_PATH))
    return parser.parse_args()


def main() -> None:
    evaluate(parse_args())


if __name__ == "__main__":
    main()
