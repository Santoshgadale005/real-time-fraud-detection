# Day 8 Summary Report: Historical Data Preparation & Data Preprocessing

**Project**: Real-Time Financial Fraud Detection Pipeline  
**Date**: July 2, 2026  
**Author**: Santoshgadale005  

---

## 1. Objectives Completed

Today we designed, implemented, and executed a modular, production-ready data preprocessing pipeline to prepare historical transaction data for training anomaly detection models.

* **Dedicated Training Directory Structure**: Structured `models/training/` to separate offline model training and preprocessing components.
* **Modular Pipeline Script**: Created `models/training/preprocess.py` containing clean, testable preprocessing functions.
* **Missing Value & Duplicate Auditing**: Verified zero missing values and zero duplicate records in the PaySim subset.
* **Feature Pruning**: Dropped structural metadata column identifiers (`nameOrig`, `nameDest`) and rule-based identifiers (`isFlaggedFraud`) to prevent algorithm overfitting and data leakage.
* **Categorical Feature Encoding**: Encoded transaction `type` into numeric columns using one-hot encoding (`pd.get_dummies(..., drop_first=True)`).
* **Numerical Feature Scaling**: Scaled numeric attributes using scikit-learn's `StandardScaler` and saved the fitted state for downstream prediction tasks.
* **Train-Test Stratified Split**: Performed an 80/20 train/test split, ensuring the extreme fraud ratio is identically distributed in both training and test sets.
* **Processed Dataset Export**: Saved the processed features and targets as separate, versioned CSV files.

---

## 2. Files Created & Updated

| File | Action | Description |
|------|--------|-------------|
| `models/training/preprocess.py` | **Created** | Modular preprocessing script with functions for loading, cleaning, dummy encoding, scaling, splitting, and saving. |
| `models/scaler.pkl` | **Created** | Saved joblib StandardScaler binary to scale live streaming Kafka events with the identical parameters. |
| `data/processed/X_train.csv` | **Created** | Training feature matrix (80,000 samples). |
| `data/processed/X_test.csv` | **Created** | Testing feature matrix (20,000 samples). |
| `data/processed/y_train.csv` | **Created** | Training target labels. |
| `data/processed/y_test.csv` | **Created** | Testing target labels. |
| `README.md` | **Updated** | Documented Day 8 progress, data preprocessing pipeline steps, and feature mappings. |
| `day_8_summary.md` | **Created** | This report. |

---

## 3. Data Cleaning and Feature engineering Details

### Removed Columns
* `nameOrig`: Ignored because customer IDs do not represent transaction patterns and cause high-cardinality dimensionality issues.
* `nameDest`: Ignored for identical reasons.
* `isFlaggedFraud`: Dropped because it is a rule-based threshold warning rather than a real target label, which would introduce structural leakage.

### Categorical One-Hot Encoding
Categorical `type` attribute values (`TRANSFER`, `PAYMENT`, `CASH_OUT`, `CASH_IN`, `DEBIT`) were encoded. By dropping the first category alphabetically (`CASH_IN`), four binary indicator features were created:
* `type_CASH_OUT`
* `type_DEBIT`
* `type_PAYMENT`
* `type_TRANSFER`

*Note: Pandas `dtype=int` was enforced so dummy columns are generated as `1` and `0` integers rather than raw booleans, matching scikit-learn input constraints.*

---

## 4. Class Distribution Statistics

The PaySim dataset is highly imbalanced, reflecting actual banking fraud rates:
* **Total Transactions**: 100,000
* **Non-Fraud Transactions**: 99,870 (99.87%)
* **Fraudulent Transactions**: 130 (0.13%)

We applied stratified splitting to ensure the train and test subsets retain this exact class imbalance:
* **y_train**: 80,000 samples (104 Fraud, 79,896 Non-Fraud)
* **y_test**: 20,000 samples (26 Fraud, 19,974 Non-Fraud)

---

## 5. Preprocessing Execution

Running the preprocessing pipeline:
```bash
venv/bin/python3 models/training/preprocess.py
```

### Log Output
```text
2026-07-02 02:51:14 [INFO] fraud-preprocessing — Loading dataset from .../data/historical/paysim.csv ...
2026-07-02 02:51:14 [INFO] fraud-preprocessing — Dataset loaded successfully. Shape: (100000, 11)
2026-07-02 02:51:14 [INFO] fraud-preprocessing — No duplicate records found.
2026-07-02 02:51:14 [INFO] fraud-preprocessing — Total missing values across all columns: 0
2026-07-02 02:51:14 [INFO] fraud-preprocessing — Removing identifier/unused columns: ['nameOrig', 'nameDest', 'isFlaggedFraud']
2026-07-02 02:51:14 [INFO] fraud-preprocessing — Encoding categorical transaction 'type' column ...
2026-07-02 02:51:14 [INFO] fraud-preprocessing — Encoded columns created: ['type_CASH_OUT', 'type_DEBIT', 'type_PAYMENT', 'type_TRANSFER']
2026-07-02 02:51:14 [INFO] fraud-preprocessing — Class distribution: Non-Fraud=99870, Fraud=130 (0.1300% Fraud)
2026-07-02 02:51:14 [INFO] fraud-preprocessing — Scaling features using StandardScaler ...
2026-07-02 02:51:15 [INFO] fraud-preprocessing — Saving fitted scaler to .../models/scaler.pkl ...
2026-07-02 02:51:15 [INFO] fraud-preprocessing — Splitting dataset into 80% training and 20% testing sets (random_state=42) ...
2026-07-02 02:51:15 [INFO] fraud-preprocessing — Training size: (80000, 10) | Testing size: (20000, 10)
2026-07-02 02:51:15 [INFO] fraud-preprocessing — Saving processed datasets to .../data/processed ...
2026-07-02 02:51:15 [INFO] fraud-preprocessing — All datasets successfully exported.
2026-07-02 02:51:15 [INFO] fraud-preprocessing — 🎉 Preprocessing pipeline complete!
```

---

## 6. Connection to Tomorrow's Work

With scaled train/test splits and our saved scaler binary, we are ready for **Day 9: Anomaly Detection with Isolation Forest**. We will train an Isolation Forest model on `X_train.csv` to detect fraud anomalies and evaluate its performance against `y_train.csv`.
