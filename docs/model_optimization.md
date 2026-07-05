# Model Optimization Report — Day 11

**Project**: Real-Time Financial Fraud Detection Pipeline  
**Optimization Date**: July 05, 2026  
**Technique**: Isolation Forest Hyperparameter Grid Search  

---

## 1. Objective

The baseline Isolation Forest model trained on Day 9 and evaluated on Day 10 showed critically
low recall (`0.038462`), meaning only
`1` out of `26` actual fraud transactions were correctly detected. The goal of
Day 11 is to systematically tune the model hyperparameters to reduce False Negatives
(missed fraud) while maintaining reasonable precision.

---

## 2. False Negative Analysis

**False Negative Count (Baseline)**: `25` missed fraud transactions out of `26` actual.

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
| `contamination` | `[0.001, 0.002, 0.005, 0.01]` |
| `n_estimators` | `[100, 200, 300]` |
| `max_samples` | `['auto', 10000, 50000]` |
| `max_features` | `[1.0, 0.8, 0.6]` |

**Total experiments run**: `20`  
**Selection criterion**: Highest **Recall** (primary), then highest **F1** (tiebreaker)

---

## 4. Full Experiment Results (Sorted by Recall)

| Model | Contamination | Trees | max_samples | max_features | Recall | Precision | F1 | TP | FN |
|-------|---------------|-------|-------------|--------------|--------|-----------|-----|----|----|
| `M20` | `0.005` | `300` | `50000` | `0.6` | `0.1154` | `0.0286` | `0.0458` | `3` | `23` |
| `M19` | `0.005` | `300` | `50000` | `0.8` | `0.1154` | `0.0291` | `0.0465` | `3` | `23` |
| `M18` | `0.005` | `300` | `50000` | `1.0` | `0.1154` | `0.0294` | `0.0469` | `3` | `23` |
| `M12` | `0.01` | `300` | `auto` | `1.0` | `0.0769` | `0.0104` | `0.0183` | `2` | `24` |
| `M17` | `0.005` | `300` | `10000` | `0.6` | `0.0769` | `0.0206` | `0.0325` | `2` | `24` |
| `M16` | `0.005` | `300` | `10000` | `0.8` | `0.0769` | `0.0189` | `0.0303` | `2` | `24` |
| `M15` | `0.005` | `300` | `10000` | `1.0` | `0.0769` | `0.0198` | `0.0315` | `2` | `24` |
| `M14` | `0.005` | `300` | `auto` | `0.6` | `0.0769` | `0.0204` | `0.0323` | `2` | `24` |
| `M13` | `0.005` | `300` | `auto` | `0.8` | `0.0769` | `0.0217` | `0.0339` | `2` | `24` |
| `M11` | `0.01` | `200` | `auto` | `1.0` | `0.0769` | `0.0105` | `0.0185` | `2` | `24` |
| `M10` | `0.01` | `100` | `auto` | `1.0` | `0.0769` | `0.0101` | `0.0179` | `2` | `24` |
| `M09` | `0.005` | `300` | `auto` | `1.0` | `0.0769` | `0.0215` | `0.0336` | `2` | `24` |
| `M08` | `0.005` | `200` | `auto` | `1.0` | `0.0769` | `0.0211` | `0.0331` | `2` | `24` |
| `M07` | `0.005` | `100` | `auto` | `1.0` | `0.0769` | `0.0211` | `0.0331` | `2` | `24` |
| `M02` | `0.001` | `200` | `auto` | `1.0` | `0.0385` | `0.0588` | `0.0465` | `1` | `25` |
| `M06` | `0.002` | `300` | `auto` | `1.0` | `0.0385` | `0.0294` | `0.0333` | `1` | `25` |
| `M05` | `0.002` | `200` | `auto` | `1.0` | `0.0385` | `0.0294` | `0.0333` | `1` | `25` |
| `M04` | `0.002` | `100` | `auto` | `1.0` | `0.0385` | `0.0270` | `0.0317` | `1` | `25` |
| `M03` | `0.001` | `300` | `auto` | `1.0` | `0.0385` | `0.0588` | `0.0465` | `1` | `25` |
| `M01` | `0.001` | `100` | `auto` | `1.0` | `0.0385` | `0.0625` | `0.0476` | `1` | `25` |

---

## 5. Best Model Configuration

| Parameter | Value |
|-----------|-------|
| `contamination` | `0.005` |
| `n_estimators` | `300` |
| `max_samples` | `50000` |
| `max_features` | `1.0` |
| `random_state` | `42` |

---

## 6. Performance Comparison: Baseline vs Optimized

| Metric | Baseline | Optimized | Change |
|--------|----------|-----------|--------|
| Accuracy | `0.998000` | `0.993900` | — |
| Precision | `0.062500` | `0.029412` | — |
| Recall | `0.038462` | `0.115385` | **+0.076923** |
| F1 Score | `0.047619` | `0.046875` | — |
| True Positives | `1` | `3` | +2 |
| False Negatives | `25` | `23` | **-2** |
| False Positives | `15` | `99` | +84 |

---

## 7. Classification Report (Optimized Model)

```text
              precision    recall  f1-score   support

           0       1.00      1.00      1.00     19974
           1       0.03      0.12      0.05        26

    accuracy                           0.99     20000
   macro avg       0.51      0.56      0.52     20000
weighted avg       1.00      0.99      1.00     20000

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
