# Deployment Package — Real-Time Financial Fraud Detection

**Model**: Isolation Forest Fraud Detector v1.0.0  
**Algorithm**: `sklearn.ensemble.IsolationForest`  
**Stage**: Production  
**Training Date**: 2026-07-06  

---

## Required Files

| File | Purpose |
|------|---------|
| `artifacts/production_model.pkl` | Trained Isolation Forest model |
| `artifacts/scaler.pkl` | Fitted StandardScaler for preprocessing |
| `artifacts/model_config.json` | Model metadata and schema |
| `artifacts/predict.py` | Self-contained prediction module |
| `config/model_config.json` | Deployment configuration |
| `scripts/validate_pipeline.py` | End-to-end pipeline validation |

---

## Dependencies

```bash
pip install scikit-learn joblib numpy pandas
```

Exact versions (from `requirements.txt` at project root):

```
scikit-learn>=1.3.0
joblib>=1.3.0
numpy>=1.24.0
pandas>=2.0.0
```

---

## How to Load the Model

```python
import joblib

model  = joblib.load("deployment/artifacts/production_model.pkl")
scaler = joblib.load("deployment/artifacts/scaler.pkl")
```

---

## Expected Input Format

A transaction must be passed as a Python dictionary with **exactly** these 10 features
in **this exact order**:

```python
transaction = {
    "step":           1,         # int   — hour of simulation
    "amount":         9839.64,   # float — transaction amount
    "oldbalanceOrg":  170136.0,  # float — sender balance before
    "newbalanceOrig": 160296.36, # float — sender balance after
    "oldbalanceDest": 0.0,       # float — receiver balance before
    "newbalanceDest": 0.0,       # float — receiver balance after
    "type_CASH_OUT":  0,         # int   — 1 if CASH_OUT, else 0
    "type_DEBIT":     0,         # int   — 1 if DEBIT, else 0
    "type_PAYMENT":   1,         # int   — 1 if PAYMENT, else 0
    "type_TRANSFER":  0,         # int   — 1 if TRANSFER, else 0
}
```

> ⚠️ **Feature order is critical.** The model was trained with these features in this
> exact sequence. Changing the order will produce incorrect predictions.

---

## Output Format

```python
{
    "prediction":    0,          # int   — 1 = FRAUD, 0 = NORMAL
    "anomaly_score": 0.043210,   # float — decision score (negative = more anomalous)
    "is_fraud":      False,      # bool  — True if prediction is fraud
    "label":         "NORMAL"    # str   — "FRAUD" or "NORMAL"
}
```

---

## Quick Start — Single Prediction

```python
import sys
sys.path.insert(0, "/path/to/real-time-fraud-detection")

from models.predict import predict_transaction

transaction = {
    "step": 1, "amount": 9839.64,
    "oldbalanceOrg": 170136.0, "newbalanceOrig": 160296.36,
    "oldbalanceDest": 0.0, "newbalanceDest": 0.0,
    "type_CASH_OUT": 0, "type_DEBIT": 0,
    "type_PAYMENT": 1, "type_TRANSFER": 0,
}

result = predict_transaction(transaction)
print(result)
# → {"prediction": 0, "anomaly_score": 0.043, "is_fraud": False, "label": "NORMAL"}
```

---

## Quick Start — Batch Prediction

```python
from models.predict import predict_batch

transactions = [transaction_1, transaction_2, transaction_3]
results = predict_batch(transactions)

for r in results:
    print(r["label"], r["anomaly_score"])
```

---

## Quick Start — Feature Validation

```python
from models.predict import validate_features

check = validate_features(transaction)
if not check["valid"]:
    print("Errors:", check["errors"])
else:
    print("Transaction is valid")
```

---

## Run End-to-End Validation

```bash
venv/bin/python deployment/scripts/validate_pipeline.py
```

Expected output:

```
✅ PASSED  Artifact Loading
✅ PASSED  Normal Transaction Prediction
✅ PASSED  Fraud Transaction Prediction
✅ PASSED  Batch Predictions
✅ PASSED  Prediction Consistency
✅ PASSED  Inference Latency
✅ PASSED  Feature Order Validation
✅ PASSED  Feature Validation Function
✅ PASSED  Full Pipeline Validation

🎉 ALL VALIDATION STEPS PASSED.
```

---

## Preprocessing Workflow

Incoming transactions must follow this preprocessing pipeline (identical to training):

```
Raw Transaction Dictionary
        ↓
Feature Validation (validate_features)
        ↓
Feature Ordering (EXPECTED_FEATURES list)
        ↓
StandardScaler Transformation (scaler.pkl)
        ↓
Isolation Forest Predict (production_model.pkl)
        ↓
Output: prediction, anomaly_score, is_fraud, label
```

---

## Model Performance

| Metric | Value |
|--------|-------|
| Accuracy | 0.9939 |
| Precision | 0.0294 |
| Recall | 0.1154 |
| F1 Score | 0.0469 |
| Throughput | **89,681 txn/sec** |
| Per-transaction latency | **0.011 ms** |

---

## Spark Integration Notes

When integrating with Apache Spark Structured Streaming:

1. Broadcast the model and scaler using `SparkContext.broadcast()`
2. Define a UDF (User-Defined Function) wrapping `predict_transaction()`
3. Apply the UDF to each Kafka message batch
4. Feature order in Spark schema must exactly match `EXPECTED_FEATURES`

Example Spark UDF skeleton:

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, BooleanType, StringType

from models.predict import predict_transaction

OUTPUT_SCHEMA = StructType([
    StructField("prediction",    IntegerType(), False),
    StructField("anomaly_score", FloatType(),   False),
    StructField("is_fraud",      BooleanType(), False),
    StructField("label",         StringType(),  False),
])

fraud_udf = udf(lambda row: predict_transaction(row.asDict()), OUTPUT_SCHEMA)
```

---

## Version History

| Version | Date | Notes |
|---------|------|-------|
| 1.0.0 | 2026-07-06 | Production-ready Isolation Forest (contamination=0.005, n_estimators=300) |

---

## Contact

Project: Real-Time Financial Fraud Detection Pipeline  
Week 2 — Day 13: Production Validation & Deployment Readiness
