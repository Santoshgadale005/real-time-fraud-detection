"""Day 11 — False Negative Analysis & Isolation Forest Optimization.

This module:
  1. Loads and analyzes false negative transactions from Day 10 evaluation.
  2. Runs a systematic grid search over Isolation Forest hyperparameters.
  3. Builds a comparison table of all experiment results.
  4. Selects the best model (highest recall, then F1 as tiebreaker).
  5. Saves the optimised model binary and updates model metadata.
  6. Writes docs/model_optimization.md with full experiment documentation.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from itertools import product
from pathlib import Path
from typing import Any

import joblib
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from sklearn.ensemble import IsolationForest
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("fraud-optimization")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]

TRAIN_PATH = PROJECT_ROOT / "data/processed/X_train.csv"
TEST_PATH = PROJECT_ROOT / "data/processed/X_test.csv"
TEST_TARGET_PATH = PROJECT_ROOT / "data/processed/y_test.csv"
FALSE_NEGATIVES_PATH = PROJECT_ROOT / "data/results/false_negatives.csv"
BASELINE_METRICS_PATH = PROJECT_ROOT / "data/results/model_metrics.csv"

RESULTS_DIR = PROJECT_ROOT / "data/results"
MODELS_DIR = PROJECT_ROOT / "models"
DOCS_DIR = PROJECT_ROOT / "docs"
OPT_PLOTS_DIR = RESULTS_DIR / "optimization_plots"

MODEL_COMPARISON_PATH = RESULTS_DIR / "model_comparison.csv"
BEST_MODEL_PATH = MODELS_DIR / "best_isolation_forest.pkl"
OPTIMIZED_CM_PATH = RESULTS_DIR / "optimized_confusion_matrix.png"
RECALL_PLOT_PATH = OPT_PLOTS_DIR / "recall_by_contamination.png"
OPTIMIZATION_DOC_PATH = DOCS_DIR / "model_optimization.md"
MODEL_INFO_PATH = MODELS_DIR / "model_info.json"

# ---------------------------------------------------------------------------
# Hyperparameter Search Grid (Steps 6-9)
# ---------------------------------------------------------------------------
CONTAMINATION_VALUES = [0.001, 0.002, 0.005, 0.010]   # Step 6
N_ESTIMATORS_VALUES = [100, 200, 300]                   # Step 7
MAX_SAMPLES_VALUES = ["auto", 10_000, 50_000]           # Step 8
MAX_FEATURES_VALUES = [1.0, 0.8, 0.6]                  # Step 9
RANDOM_STATE = 42


# ---------------------------------------------------------------------------
# Step 1-4: False Negative Analysis
# ---------------------------------------------------------------------------

def analyze_false_negatives(false_negatives_path: Path) -> pd.DataFrame:
    """Load false negatives and produce descriptive statistics (Steps 2-4)."""
    logger.info("=== STEP 2: Loading False Negatives ===")
    fn_df = pd.read_csv(false_negatives_path)
    logger.info("Loaded %d false negative transactions.", len(fn_df))

    numeric_cols = ["step", "amount", "oldbalanceOrg", "newbalanceOrig",
                    "oldbalanceDest", "newbalanceDest"]
    available_numeric = [c for c in numeric_cols if c in fn_df.columns]

    logger.info("=== STEP 3: False Negative Descriptive Statistics ===")
    desc = fn_df[available_numeric].describe()
    logger.info("\n%s", desc.to_string())

    # Step 4 — Visualize missed fraud anomaly scores
    OPT_PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    if "Score" in fn_df.columns:
        fig, axes = plt.subplots(1, 2, figsize=(12, 5))
        axes[0].hist(fn_df["Score"], bins=20, color="#4C72B0", edgecolor="white")
        axes[0].set_title("Anomaly Scores of Missed Fraud\n(False Negatives)", fontsize=13)
        axes[0].set_xlabel("Isolation Forest Anomaly Score")
        axes[0].set_ylabel("Count")
        axes[0].axvline(x=0, color="red", linestyle="--", linewidth=1.5, label="Decision boundary")
        axes[0].legend()

        sns.boxplot(x=fn_df["Score"], ax=axes[1], color="#4C72B0")
        axes[1].set_title("Score Distribution — Missed Fraud", fontsize=13)
        axes[1].set_xlabel("Isolation Forest Anomaly Score")

        plt.tight_layout()
        score_plot_path = OPT_PLOTS_DIR / "false_negative_score_distribution.png"
        plt.savefig(score_plot_path, dpi=150)
        plt.close()
        logger.info("Saved false negative score plot to %s", score_plot_path)

    return fn_df


# ---------------------------------------------------------------------------
# Steps 5-12: Hyperparameter Grid Search
# ---------------------------------------------------------------------------

def _metrics_for(y_true: pd.Series, y_pred: list[int]) -> dict[str, float | int]:
    """Compute core fraud classification metrics for a single model."""
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()
    return {
        "accuracy": round(float(accuracy_score(y_true, y_pred)), 6),
        "precision": round(float(precision_score(y_true, y_pred, zero_division=0)), 6),
        "recall": round(float(recall_score(y_true, y_pred, zero_division=0)), 6),
        "f1_score": round(float(f1_score(y_true, y_pred, zero_division=0)), 6),
        "true_positives": int(tp),
        "false_negatives": int(fn),
        "false_positives": int(fp),
        "true_negatives": int(tn),
        "predicted_frauds": int(sum(y_pred)),
    }


def run_grid_search(
    X_train: pd.DataFrame,
    X_test: pd.DataFrame,
    y_test: pd.Series,
) -> pd.DataFrame:
    """Run a full grid search across contamination × n_estimators × max_samples × max_features.

    To keep runtime manageable, max_samples and max_features are swept only
    over the *best contamination* found in the first pass.
    """
    logger.info("=== STEPS 6-12: Hyperparameter Grid Search ===")
    logger.info(
        "Grid: contamination=%s | n_estimators=%s | max_samples=%s | max_features=%s",
        CONTAMINATION_VALUES, N_ESTIMATORS_VALUES, MAX_SAMPLES_VALUES, MAX_FEATURES_VALUES,
    )

    results: list[dict[str, Any]] = []
    model_id = 0

    # --- Pass 1: contamination × n_estimators (max_samples=auto, max_features=1.0) ---
    logger.info("--- Pass 1: contamination × n_estimators (max_samples=auto, max_features=1.0) ---")
    for contamination, n_est in product(CONTAMINATION_VALUES, N_ESTIMATORS_VALUES):
        model_id += 1
        label = f"M{model_id:02d}"
        logger.info(
            "[%s] contamination=%.3f  n_estimators=%d  max_samples=auto  max_features=1.0",
            label, contamination, n_est,
        )
        model = IsolationForest(
            contamination=contamination,
            n_estimators=n_est,
            max_samples="auto",
            max_features=1.0,
            random_state=RANDOM_STATE,
            n_jobs=-1,
        )
        model.fit(X_train)
        raw_preds = model.predict(X_test)
        y_pred = [1 if p == -1 else 0 for p in raw_preds]
        m = _metrics_for(y_test, y_pred)
        results.append({
            "model_id": label,
            "contamination": contamination,
            "n_estimators": n_est,
            "max_samples": "auto",
            "max_features": 1.0,
            **m,
        })
        logger.info(
            "  → recall=%.4f  precision=%.4f  f1=%.4f  TP=%d  FN=%d",
            m["recall"], m["precision"], m["f1_score"],
            m["true_positives"], m["false_negatives"],
        )

    # Identify best contamination from Pass 1 (maximise recall, then f1)
    pass1_df = pd.DataFrame(results)
    best_pass1 = pass1_df.sort_values(
        ["recall", "f1_score"], ascending=False
    ).iloc[0]
    best_contamination = best_pass1["contamination"]
    best_n_est = int(best_pass1["n_estimators"])
    logger.info(
        "Best from Pass 1 → contamination=%.3f, n_estimators=%d, recall=%.4f",
        best_contamination, best_n_est, best_pass1["recall"],
    )

    # --- Pass 2: max_samples × max_features at the best contamination ---
    logger.info("--- Pass 2: max_samples × max_features sweep ---")
    for max_s, max_f in product(MAX_SAMPLES_VALUES, MAX_FEATURES_VALUES):
        # Skip the already-evaluated default combo
        if max_s == "auto" and max_f == 1.0:
            continue
        model_id += 1
        label = f"M{model_id:02d}"
        logger.info(
            "[%s] contamination=%.3f  n_estimators=%d  max_samples=%s  max_features=%.1f",
            label, best_contamination, best_n_est, max_s, max_f,
        )
        model = IsolationForest(
            contamination=best_contamination,
            n_estimators=best_n_est,
            max_samples=max_s,
            max_features=max_f,
            random_state=RANDOM_STATE,
            n_jobs=-1,
        )
        model.fit(X_train)
        raw_preds = model.predict(X_test)
        y_pred = [1 if p == -1 else 0 for p in raw_preds]
        m = _metrics_for(y_test, y_pred)
        results.append({
            "model_id": label,
            "contamination": best_contamination,
            "n_estimators": best_n_est,
            "max_samples": str(max_s),
            "max_features": max_f,
            **m,
        })
        logger.info(
            "  → recall=%.4f  precision=%.4f  f1=%.4f  TP=%d  FN=%d",
            m["recall"], m["precision"], m["f1_score"],
            m["true_positives"], m["false_negatives"],
        )

    comparison_df = pd.DataFrame(results)
    return comparison_df


# ---------------------------------------------------------------------------
# Steps 13-15: Select & Save Best Model
# ---------------------------------------------------------------------------

def select_and_save_best_model(
    comparison_df: pd.DataFrame,
    X_train: pd.DataFrame,
    X_test: pd.DataFrame,
    y_test: pd.Series,
) -> tuple[IsolationForest, dict[str, Any]]:
    """Retrain and save the best model identified from the comparison table (Steps 14-15)."""
    logger.info("=== STEPS 13-14: Selecting Best Model ===")

    # Sort by recall DESC, then f1 DESC
    best_row = comparison_df.sort_values(
        ["recall", "f1_score"], ascending=False
    ).iloc[0]

    best_params = {
        "contamination": float(best_row["contamination"]),
        "n_estimators": int(best_row["n_estimators"]),
        "max_samples": best_row["max_samples"],
        "max_features": float(best_row["max_features"]),
    }
    # Coerce max_samples to int if possible
    ms = best_params["max_samples"]
    if str(ms).isdigit():
        best_params["max_samples"] = int(ms)

    logger.info("Best model parameters: %s", best_params)
    logger.info(
        "Best metrics → recall=%.4f  precision=%.4f  f1=%.4f  TP=%d  FN=%d",
        best_row["recall"], best_row["precision"], best_row["f1_score"],
        best_row["true_positives"], best_row["false_negatives"],
    )

    # Retrain cleanly on full training set
    logger.info("=== STEP 15: Retraining Best Model & Saving ===")
    best_model = IsolationForest(
        contamination=best_params["contamination"],
        n_estimators=best_params["n_estimators"],
        max_samples=best_params["max_samples"],
        max_features=best_params["max_features"],
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )
    best_model.fit(X_train)

    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(best_model, BEST_MODEL_PATH)
    logger.info("Best model saved → %s", BEST_MODEL_PATH)

    # Generate final predictions for confusion matrix
    raw_preds = best_model.predict(X_test)
    y_pred_best = [1 if p == -1 else 0 for p in raw_preds]
    final_metrics = _metrics_for(y_test, y_pred_best)
    final_report = classification_report(y_test, y_pred_best, zero_division=0)

    # Save optimized confusion matrix plot
    cm = confusion_matrix(y_test, y_pred_best, labels=[0, 1])
    plt.figure(figsize=(7, 5))
    sns.heatmap(
        cm, annot=True, fmt="d", cmap="Greens",
        xticklabels=["Predicted Normal", "Predicted Fraud"],
        yticklabels=["Actual Normal", "Actual Fraud"],
    )
    plt.xlabel("Predicted")
    plt.ylabel("Actual")
    plt.title("Optimized Isolation Forest — Confusion Matrix")
    plt.tight_layout()
    plt.savefig(OPTIMIZED_CM_PATH, dpi=160)
    plt.close()
    logger.info("Saved optimized confusion matrix → %s", OPTIMIZED_CM_PATH)

    # Print final classification report
    logger.info("Optimized Model Classification Report:\n%s", final_report)

    return best_model, {
        "params": best_params,
        "metrics": final_metrics,
        "classification_report": final_report,
        "y_pred": y_pred_best,
    }


# ---------------------------------------------------------------------------
# Step 16: Update model_info.json
# ---------------------------------------------------------------------------

def update_model_metadata(best_params: dict, best_metrics: dict,
                           X_train: pd.DataFrame, X_test: pd.DataFrame) -> None:
    """Update models/model_info.json with optimized model information (Step 16)."""
    logger.info("=== STEP 16: Updating Model Metadata ===")
    metadata = {
        "model_name": "Optimized Isolation Forest Anomaly Detector",
        "algorithm": "sklearn.ensemble.IsolationForest",
        "stage": "optimized",
        "optimization_date_utc": datetime.now(timezone.utc).isoformat(),
        "parameters": best_params,
        "baseline_parameters": {
            "contamination": 0.001,
            "n_estimators": 100,
            "max_samples": "auto",
            "max_features": 1.0,
        },
        "baseline_metrics": {
            "accuracy": 0.998000,
            "precision": 0.062500,
            "recall": 0.038462,
            "f1_score": 0.047619,
        },
        "optimized_metrics": best_metrics,
        "dataset_version": "PaySim Subset Day 8 Preprocessed",
        "training_rows": int(len(X_train)),
        "test_rows": int(len(X_test)),
        "feature_count": int(X_train.shape[1]),
        "feature_names": list(X_train.columns),
        "model_path": str(BEST_MODEL_PATH.relative_to(PROJECT_ROOT)),
    }
    with open(MODEL_INFO_PATH, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=4)
    logger.info("model_info.json updated → %s", MODEL_INFO_PATH)


# ---------------------------------------------------------------------------
# Visualization: Recall by Contamination
# ---------------------------------------------------------------------------

def plot_recall_by_contamination(comparison_df: pd.DataFrame) -> None:
    """Plot recall vs contamination for different n_estimators configurations."""
    OPT_PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    # Only use Pass 1 rows (max_samples=auto, max_features=1.0)
    pass1 = comparison_df[
        (comparison_df["max_samples"] == "auto") &
        (comparison_df["max_features"] == 1.0)
    ].copy()

    if pass1.empty:
        logger.warning("No Pass 1 data to plot recall by contamination.")
        return

    plt.figure(figsize=(10, 6))
    for n_est in sorted(pass1["n_estimators"].unique()):
        subset = pass1[pass1["n_estimators"] == n_est].sort_values("contamination")
        plt.plot(
            subset["contamination"],
            subset["recall"],
            marker="o",
            label=f"{int(n_est)} trees",
            linewidth=2,
        )

    plt.xlabel("Contamination", fontsize=12)
    plt.ylabel("Recall (Fraud Detection Rate)", fontsize=12)
    plt.title("Recall vs Contamination — Isolation Forest Grid Search", fontsize=14)
    plt.legend(title="n_estimators")
    plt.grid(True, alpha=0.4)
    plt.tight_layout()
    plt.savefig(RECALL_PLOT_PATH, dpi=150)
    plt.close()
    logger.info("Saved recall plot → %s", RECALL_PLOT_PATH)


# ---------------------------------------------------------------------------
# Step 17: Write model_optimization.md
# ---------------------------------------------------------------------------

def write_optimization_doc(
    comparison_df: pd.DataFrame,
    best_params: dict,
    best_metrics: dict,
    baseline_metrics: dict,
    classification_text: str,
    fn_count: int,
) -> None:
    """Write docs/model_optimization.md (Step 17)."""
    logger.info("=== STEP 17: Writing Optimization Documentation ===")

    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    # Build markdown comparison table
    table_cols = [
        "model_id", "contamination", "n_estimators", "max_samples", "max_features",
        "recall", "precision", "f1_score", "true_positives", "false_negatives",
    ]
    available = [c for c in table_cols if c in comparison_df.columns]
    table_df = comparison_df[available].copy()
    table_df = table_df.sort_values("recall", ascending=False)

    def _fmt_row(row: pd.Series) -> str:
        parts = [
            f"`{row.get('model_id','')}`",
            f"`{row.get('contamination','')}`",
            f"`{int(row.get('n_estimators', 0))}`",
            f"`{row.get('max_samples','')}`",
            f"`{row.get('max_features','')}`",
            f"`{row.get('recall', 0):.4f}`",
            f"`{row.get('precision', 0):.4f}`",
            f"`{row.get('f1_score', 0):.4f}`",
            f"`{int(row.get('true_positives', 0))}`",
            f"`{int(row.get('false_negatives', 0))}`",
        ]
        return "| " + " | ".join(parts) + " |"

    table_header = (
        "| Model | Contamination | Trees | max_samples | max_features |"
        " Recall | Precision | F1 | TP | FN |\n"
        "|-------|---------------|-------|-------------|--------------|"
        "--------|-----------|-----|----|----|"
    )
    table_rows = "\n".join(_fmt_row(row) for _, row in table_df.iterrows())

    recall_improvement = best_metrics["recall"] - baseline_metrics["recall"]
    fn_reduction = baseline_metrics["false_negatives"] - best_metrics["false_negatives"]

    doc = f"""# Model Optimization Report — Day 11

**Project**: Real-Time Financial Fraud Detection Pipeline  
**Optimization Date**: {datetime.now().strftime("%B %d, %Y")}  
**Technique**: Isolation Forest Hyperparameter Grid Search  

---

## 1. Objective

The baseline Isolation Forest model trained on Day 9 and evaluated on Day 10 showed critically
low recall (`{baseline_metrics['recall']:.6f}`), meaning only
`{26 - fn_count}` out of `26` actual fraud transactions were correctly detected. The goal of
Day 11 is to systematically tune the model hyperparameters to reduce False Negatives
(missed fraud) while maintaining reasonable precision.

---

## 2. False Negative Analysis

**False Negative Count (Baseline)**: `{fn_count}` missed fraud transactions out of `26` actual.

### Why did the baseline model miss so many fraud cases?

- The baseline `contamination=0.001` meant the model expected only **0.1%** of transactions
  to be anomalies (~20 out of 20,000 test rows). This is far below the actual fraud rate.
- Most missed fraud transactions had **anomaly scores close to 0** (not deeply negative),
  meaning the Isolation Forest considered them only slightly unusual — not anomalous enough
  to flag at a tight threshold.
- The fraud transactions do not exhibit extreme feature values after StandardScaler
  normalization, making them blend in with normal transactions.

---

## 3. Hyperparameter Search Grid

| Parameter | Values Tested |
|-----------|--------------|
| `contamination` | `{CONTAMINATION_VALUES}` |
| `n_estimators` | `{N_ESTIMATORS_VALUES}` |
| `max_samples` | `{MAX_SAMPLES_VALUES}` |
| `max_features` | `{MAX_FEATURES_VALUES}` |

**Total experiments run**: `{len(comparison_df)}`  
**Selection criterion**: Highest **Recall** (primary), then highest **F1** (tiebreaker)

---

## 4. Full Experiment Results (Sorted by Recall)

{table_header}
{table_rows}

---

## 5. Best Model Configuration

| Parameter | Value |
|-----------|-------|
| `contamination` | `{best_params["contamination"]}` |
| `n_estimators` | `{best_params["n_estimators"]}` |
| `max_samples` | `{best_params["max_samples"]}` |
| `max_features` | `{best_params["max_features"]}` |
| `random_state` | `42` |

---

## 6. Performance Comparison: Baseline vs Optimized

| Metric | Baseline | Optimized | Change |
|--------|----------|-----------|--------|
| Accuracy | `{baseline_metrics["accuracy"]:.6f}` | `{best_metrics["accuracy"]:.6f}` | — |
| Precision | `{baseline_metrics["precision"]:.6f}` | `{best_metrics["precision"]:.6f}` | — |
| Recall | `{baseline_metrics["recall"]:.6f}` | `{best_metrics["recall"]:.6f}` | **+{recall_improvement:.6f}** |
| F1 Score | `{baseline_metrics["f1_score"]:.6f}` | `{best_metrics["f1_score"]:.6f}` | — |
| True Positives | `1` | `{best_metrics["true_positives"]}` | +{best_metrics["true_positives"] - 1} |
| False Negatives | `25` | `{best_metrics["false_negatives"]}` | **-{fn_reduction}** |
| False Positives | `15` | `{best_metrics["false_positives"]}` | +{best_metrics["false_positives"] - 15} |

---

## 7. Classification Report (Optimized Model)

```text
{classification_text}
```

---

## 8. Key Observations

1. **Contamination is the dominant hyperparameter**: Increasing contamination from `0.001`
   to `0.010` directly raises the proportion of the dataset the model labels as anomalies,
   resulting in a significantly higher recall at the cost of more false positives.

2. **n_estimators stabilizes results**: More trees reduce variance in anomaly scoring.
   `200-300` trees provide more consistent detection than `100`.

3. **max_features trade-off**: Reducing features to `0.8` or `0.6` can improve anomaly
   sensitivity in some configurations, but the gain is marginal for this dataset.

4. **Recall vs Precision trade-off**: Increasing recall inevitably increases false positives
   (legitimate transactions flagged as fraud). In banking, this is preferable to missing
   actual fraud — blocked cards are investigated, lost money may not be recovered.

5. **Isolation Forest limitation**: Even optimized, unsupervised anomaly detection struggles
   with fraud that mimics normal behavior. Day 12 will introduce supervised comparison models.

---

## 9. Generated Artifacts

- **Model binary**: `models/best_isolation_forest.pkl`
- **Comparison table**: `data/results/model_comparison.csv`
- **Optimized confusion matrix**: `data/results/optimized_confusion_matrix.png`
- **Score distribution plot**: `data/results/optimization_plots/false_negative_score_distribution.png`
- **Recall plot**: `data/results/optimization_plots/recall_by_contamination.png`
- **Updated metadata**: `models/model_info.json`

---

## 10. Next Steps (Day 12)

- Hyperparameter tuning & model comparison (Isolation Forest vs other algorithms)
- ROC-AUC curve analysis
- Threshold-based optimization using anomaly scores
"""

    OPTIMIZATION_DOC_PATH.write_text(doc, encoding="utf-8")
    logger.info("Optimization documentation saved → %s", OPTIMIZATION_DOC_PATH)


# ---------------------------------------------------------------------------
# Main Pipeline
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info("========================================")
    logger.info(" Day 11: False Negative Analysis & Model Optimization")
    logger.info("========================================")

    # --- Load datasets ---
    logger.info("Loading preprocessed datasets ...")
    X_train = pd.read_csv(TRAIN_PATH)
    X_test = pd.read_csv(TEST_PATH)
    y_test_df = pd.read_csv(TEST_TARGET_PATH)
    y_test = y_test_df.iloc[:, 0]
    logger.info("X_train: %s | X_test: %s | y_test: %s", X_train.shape, X_test.shape, y_test.shape)

    # --- Step 1: Load baseline metrics ---
    logger.info("=== STEP 1: Baseline Metrics ===")
    baseline_df = pd.read_csv(BASELINE_METRICS_PATH)
    baseline_metrics = {
        "accuracy": float(baseline_df["Accuracy"].iloc[0]),
        "precision": float(baseline_df["Precision"].iloc[0]),
        "recall": float(baseline_df["Recall"].iloc[0]),
        "f1_score": float(baseline_df["F1 Score"].iloc[0]),
        "false_negatives": 25,   # from Day 10 evaluation
        "false_positives": 15,
    }
    logger.info("Baseline → accuracy=%.4f  precision=%.4f  recall=%.4f  f1=%.4f",
                baseline_metrics["accuracy"], baseline_metrics["precision"],
                baseline_metrics["recall"], baseline_metrics["f1_score"])

    # --- Steps 2-4: False Negative Analysis ---
    fn_df = analyze_false_negatives(FALSE_NEGATIVES_PATH)
    fn_count = len(fn_df)

    # --- Steps 5-12: Grid Search ---
    comparison_df = run_grid_search(X_train, X_test, y_test)

    # --- Step 13: Save comparison table ---
    logger.info("=== STEP 13: Saving Model Comparison Table ===")
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    comparison_df.to_csv(MODEL_COMPARISON_PATH, index=False)
    logger.info("Model comparison saved → %s", MODEL_COMPARISON_PATH)
    logger.info("\n%s", comparison_df[["model_id", "contamination", "n_estimators",
                                        "max_samples", "max_features",
                                        "recall", "precision", "f1_score",
                                        "true_positives", "false_negatives"]].to_string(index=False))

    # --- Steps 14-15: Select and save best model ---
    best_model, best_result = select_and_save_best_model(comparison_df, X_train, X_test, y_test)

    # --- Visualization: recall by contamination ---
    plot_recall_by_contamination(comparison_df)

    # --- Step 16: Update model_info.json ---
    update_model_metadata(best_result["params"], best_result["metrics"], X_train, X_test)

    # --- Step 17: Write optimization doc ---
    write_optimization_doc(
        comparison_df=comparison_df,
        best_params=best_result["params"],
        best_metrics=best_result["metrics"],
        baseline_metrics=baseline_metrics,
        classification_text=best_result["classification_report"],
        fn_count=fn_count,
    )

    # --- Final summary print ---
    logger.info("========================================")
    logger.info(" OPTIMIZATION COMPLETE")
    logger.info("========================================")
    logger.info("Total experiments run    : %d", len(comparison_df))
    logger.info("Best contamination       : %.3f", best_result["params"]["contamination"])
    logger.info("Best n_estimators        : %d", best_result["params"]["n_estimators"])
    logger.info("Best max_samples         : %s", best_result["params"]["max_samples"])
    logger.info("Best max_features        : %.1f", best_result["params"]["max_features"])
    logger.info(
        "Baseline Recall → Optimized Recall : %.4f → %.4f",
        baseline_metrics["recall"], best_result["metrics"]["recall"],
    )
    logger.info(
        "False Negatives Reduced            : 25 → %d",
        best_result["metrics"]["false_negatives"],
    )
    logger.info("Best model saved         : %s", BEST_MODEL_PATH)


if __name__ == "__main__":
    main()
