# PaySim Exploratory Data Analysis (EDA) Summary

This document summarizes the insights and findings obtained from analyzing the PaySim synthetic financial transactions dataset.

---

## 1. Dataset Dimensions & Size
* **Total Transactions**: 100,000 logs
* **Number of Attributes**: 11 features
* **Data Volume Scale**: In production, the full PaySim dataset consists of **6,362,620 transactions**, highlighting that processing these datasets requires scalable streaming architectures (such as Spark Structured Streaming) rather than traditional single-node batch processing.

---

## 2. Target Variable & Class Imbalance Analysis
* **Legitimate Transactions (0)**: 99,870 records
* **Fraudulent Transactions (1)**: 130 records
* **Fraud Percentage (Rate)**: **0.1300%**

> [!WARNING]
> **Severe Class Imbalance**: With a fraud rate of only 0.13%, class distribution is heavily skewed. Relying on classification **accuracy** is dangerous (a dummy model predicting "legitimate" for everything achieves 99.87% accuracy but detects 0% of fraud). Model success must be measured using **Precision**, **Recall** (Sensitivity), and **F1-Score**.

---

## 3. Key Findings: Fraud Distribution vs. Transaction Types
Analyzing transaction types and mapping them against the target column reveals a critical pattern:

| Transaction Type | Total Counts | Fraud Counts | Fraud Rate (%) |
|------------------|--------------|--------------|----------------|
| **CASH_OUT**     | 35,000       | 65           | 0.1857%        |
| **PAYMENT**      | 33,000       | 0            | 0.0000%        |
| **CASH_IN**      | 22,000       | 0            | 0.0000%        |
| **TRANSFER**     | 9,000        | 65           | 0.7222%        |
| **DEBIT**        | 1,000        | 0            | 0.0000%        |

### Analysis:
1. **Zero-Risk Categories**: Transactions categorized as `PAYMENT`, `CASH_IN`, and `DEBIT` have zero recorded cases of fraud.
2. **High-Risk Categories**: 100% of all fraudulent activity is concentrated in **`TRANSFER`** and **`CASH_OUT`**.
3. **Behavioral Fraud Loop**: Fraudsters typically execute a two-step sequence:
   * **Step 1**: Transfer funds out of the victim's account to a mule account (`TRANSFER`).
   * **Step 2**: Cash out the stolen funds from the mule account immediately (`CASH_OUT`).

---

## 4. Transaction Amounts Analysis
* **Legitimate Amounts**: Typical amounts range from small consumer payments (e.g. $10 to $500) up to larger business invoices.
* **Fraudulent Amounts**: Boxplot analysis indicates that fraud transactions are highly biased towards massive amounts (often transfering the entire account balance).
* **Statistical Distribution**: Transaction amounts are heavily right-skewed. To ensure numerical stability in our machine learning algorithms, we may apply log transformation to the amount fields.

---

## 5. Account Balances and Anomalies
A major hallmark of fraudulent behavior is the state of account balances before and after transactions:
1. **Origin Accounts (`newbalanceOrig` = 0)**: In almost all fraud cases, the sender's account balance is completely emptied (`newbalanceOrig` becomes exactly `0.0` despite the transaction amount being very large).
2. **Destination Accounts (`newbalanceDest` discrepancy)**: Legitimate transfers update destination balances correctly. In many fraudulent transactions, the destination balances (`oldbalanceDest` and `newbalanceDest`) remain `0.0`, reflecting an anomaly where funds are instantly routed away or not settled properly in the simulation logs.

---

## 6. Candidate Feature Selection for Machine Learning
Based on this EDA, the primary feature set for training our **Isolation Forest** model in Week 2 should include:

1. **`amount`**: Large values indicate high fraud risk.
2. **`oldbalanceOrg`**: Starting balance of the sender.
3. **`newbalanceOrig`**: Balance of the sender after the transaction (looking for drop to 0).
4. **`oldbalanceDest`**: Destination balance before transaction.
5. **`newbalanceDest`**: Destination balance after transaction.
6. **`type`**: Encoded categories, with specific attention given to `TRANSFER` and `CASH_OUT`.

---

## 7. Strategic Business Insights
1. **Targeted Ingestion Filter**: Since fraud is exclusively found in `TRANSFER` and `CASH_OUT` transactions, our Kafka producer and Spark engine can focus computational scoring power on these high-risk types rather than spending resources scoring low-risk payments or cash-ins.
2. **Threshold Alerts**: Transactions exceeding $200,000 show high correlation with flagged fraud alerts. We should implement threshold rules alongside anomaly detection models.
