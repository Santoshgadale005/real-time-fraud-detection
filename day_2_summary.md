# Day 2 Summary Report: PaySim Dataset Deep Dive & Fraud EDA

**Project**: Real-Time Financial Fraud Detection Pipeline  
**Date**: June 23, 2026  
**Author**: Santoshgadale005  

---

## 1. Objectives Completed
Today we focused on understanding our data asset—the PaySim synthetic financial dataset. We completed the following milestones:
* Generated a local transaction dataset with realistic distributions.
* Created and executed a Jupyter notebook containing all EDA steps and plots.
* Analyzed class imbalance, transaction types, amounts, and account balances.
* Mapped correlations and selected candidate features for model development.
* Saved a dedicated EDA summary report and pushed the files to GitHub.

---

## 2. Files Created
We created and added the following files to the project directory:
* **`data/generate_paysim.py`**: A python script that simulates 100,000 transactions mirroring true PaySim characteristics (types, values, and fraud ratios).
* **`data/paysim.csv`**: The generated dataset containing 100,000 rows (ignored in `.gitignore` to avoid bloating Git storage).
* **`notebooks/fraud_eda.ipynb`**: The Jupyter Notebook performing the step-by-step exploratory analysis, complete with computed statistics and embedded visualizations.
* **`notebooks/eda_summary.md`**: A detailed report on transaction risks, fraud loops, and candidates for machine learning features.
* **Plots (Saved in `notebooks/`)**:
  * `fraud_distribution.png`: Bar chart of the class imbalance (log scale).
  * `amount_distribution.png`: Histogram showing the skewed spread of transaction values.
  * `amount_boxplot.png`: Boxplot comparing transaction amounts between normal and fraud transactions.
  * `correlation_heatmap.png`: Heatmap demonstrating correlations between starting and ending balances.

---

## 3. Key Data Insights
1. **Extreme Class Imbalance**: Out of 100,000 transactions, only **130** are fraudulent (**0.1300%**). Accuracy is a useless evaluation metric here; we must evaluate model performance using Precision and Recall.
2. **High-Risk Transaction Types**: Fraud is concentrated entirely within **`TRANSFER`** and **`CASH_OUT`** transactions.
3. **Fraud Signature**: Fraudulent events are characterized by large amounts that completely deplete the origin account (`newbalanceOrig = 0.0`) and often exhibit zero-balance discrepancies on the destination account.

---

## 4. Git Version Control Status
All files were staged, committed, and pushed to your GitHub repository:
* **Repository**: `https://github.com/Santoshgadale005/real-time-fraud-detection`
* **Branch**: `main`
* **Commit Message**: `Day 2: PaySim Dataset Deep Dive & Fraud EDA`
