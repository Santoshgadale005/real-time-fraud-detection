# Model Evaluation Summary

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
| Accuracy | `0.998000` | Proportion of correct predictions overall. |
| Precision | `0.062500` | Proportion of predicted anomalies that were actual fraud. |
| Recall | `0.038462` | Proportion of actual fraud cases successfully detected. |
| F1 Score | `0.047619` | Harmonic mean of Precision and Recall. |

## Confusion Matrix

| Actual / Predicted | Normal | Fraud |
|--------------------|--------|-------|
| Actual Normal | `19959` | `15` |
| Actual Fraud | `25` | `1` |

Raw matrix:
```text
[[19959, 15], [25, 1]]
```

## Classification Report

```text
              precision    recall  f1-score   support

           0       1.00      1.00      1.00     19974
           1       0.06      0.04      0.05        26

    accuracy                           1.00     20000
   macro avg       0.53      0.52      0.52     20000
weighted avg       1.00      1.00      1.00     20000

```

## Prediction Counts

How many anomalies were flagged by the model:
```text
Prediction
0    19984
1       16
```

## Actual vs Predicted Fraud Cross-Tabulation

```text
Prediction      0   1
Actual               
0           19959  15
1              25   1
```

## Initial Observations

- **Accuracy is Misleading**: The baseline model achieves `99.80%` accuracy, but this is solely due to class imbalance (only `26` frauds in `20000` transactions).
- **Extremely Low Recall**: The model has a recall of `0.038462` (detecting only `1` out of `26` fraud cases). It missed `25` fraud events (False Negatives), representing high-risk financial leaks.
- **Moderate Precision**: The precision is `0.062500`, meaning only `6.25%` of the transactions flagged as anomalies were actual fraud cases.
- **Identified Weaknesses**: The baseline Isolation Forest setup (`contamination=0.001`) is too conservative. It flags only `16` total transactions as anomalies out of `20000` test rows. We need to optimize contamination and threshold boundaries on Day 11.

## Generated Artifacts

- **Metrics CSV**: `data/results/model_metrics.csv`
- **Confusion Matrix Heatmap**: `data/results/confusion_matrix.png`
