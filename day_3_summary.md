# Day 3 Summary Report: Fraud Pattern Investigation & Feature Engineering Foundations

**Project**: Real-Time Financial Fraud Detection Pipeline  
**Date**: June 24, 2026  
**Author**: Santoshgadale005  

---

## 1. Objectives Completed
Today, we shifted from high-level exploratory analysis to targeted fraud profiling and feature engineering. We achieved the following milestones:
* Isolated and analyzed fraudulent transactions to map common attack vectors.
* Discovered specific origin balance anomalies (account drainage) and destination discrepancies.
* Designed and engineered 5 new features targeting fraud signatures.
* Created and executed the feature engineering notebook with complete documentation.
* Built a reusable Python transformation module for integration with model training and Spark.
* Saved the final processed dataset `data/paysim_engineered.csv`.
* Created the Feature Dictionary document.

---

## 2. Files Created & Committed
* **`notebooks/feature_engineering.ipynb`**: Interactive notebook loading the raw data, calculating statistics, applying transformations, and verifying features.
* **`notebooks/feature_correlation.png`**: Heatmap visualizing linear relationships of all features, confirming that engineered features (`amount_balance_ratio`, `account_drained`) display strong correlations with the target label `isFraud`.
* **`models/feature_engineering.py`**: A clean, modular Python file implementing the `engineer_features(df)` function. This pipeline will be reused directly in Spark streaming jobs and model deployment scripts.
* **`data/paysim_engineered.csv`**: The complete engineered dataset with both original columns and new engineered columns (ignored in Git).
* **`docs/feature_dictionary.md`**: Definition file outlining raw columns, transformation formulas, and the business rationale for the engineered features.
* **`day_3_summary.md`**: This summary report documenting the completed day 3 deliverables.

---

## 3. Engineered Features Details
Based on our pattern investigation, we engineered the following features:
1. **`origin_balance_diff`** (`oldbalanceOrg - newbalanceOrig`): Tracks net cash leaving the sender's account.
2. **`dest_balance_diff`** (`newbalanceDest - oldbalanceDest`): Tracks net cash arriving in the destination account.
3. **`amount_balance_ratio`** (`amount / (oldbalanceOrg + 1)`): Measures the proportion of sender capital involved (identifies complete asset transfers).
4. **`account_drained`** (`newbalanceOrig == 0`): Flag tracking if the account was entirely cleared out.
5. **`high_value_txn`** (`amount > 95th_percentile`): Flags high-risk, large-sum transfers.

---

## 4. Git Version Control Status
All files were staged, committed, and pushed to your remote repository:
* **Repository**: `https://github.com/Santoshgadale005/real-time-fraud-detection`
* **Branch**: `main`
* **Commit Message**: `Day 3: Fraud Pattern Investigation & Feature Engineering Foundations`
