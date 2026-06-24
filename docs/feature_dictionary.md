# Feature Dictionary: Real-Time Fraud Detection Pipeline

This directory documents the raw and engineered features used by our fraud detection and real-time streaming pipeline.

---

## 1. Original Dataset Features

These features are extracted directly from the raw financial transaction log:

### `step`
* **Type**: Integer
* **Definition**: Represents a unit of time in the real world. 1 step is equivalent to 1 hour of simulation time.
* **Scope**: Total simulation span is 744 steps (31 days).

### `type`
* **Type**: Categorical (String)
* **Definition**: The transaction medium.
* **Categories**: `CASH_IN`, `CASH_OUT`, `DEBIT`, `PAYMENT`, `TRANSFER`.

### `amount`
* **Type**: Float (Decimal)
* **Definition**: The transaction volume (in local currency units).

### `nameOrig`
* **Type**: String
* **Definition**: Unique identifier of the transaction sender account.

### `oldbalanceOrg`
* **Type**: Float (Decimal)
* **Definition**: Starting balance of the sender account *prior* to the transaction.

### `newbalanceOrig`
* **Type**: Float (Decimal)
* **Definition**: Ending balance of the sender account *after* the transaction.

### `nameDest`
* **Type**: String
* **Definition**: Unique identifier of the recipient account. Merchant IDs start with 'M'.

### `oldbalanceDest`
* **Type**: Float (Decimal)
* **Definition**: Starting balance of the recipient account *prior* to the transaction (Note: Merchants have 0.0 value records).

### `newbalanceDest`
* **Type**: Float (Decimal)
* **Definition**: Ending balance of the recipient account *after* the transaction (Note: Merchants have 0.0 value records).

### `isFraud`
* **Type**: Binary (0 or 1)
* **Definition**: The ground truth label. `1` indicates a transaction executed by a fraudulent agent (unauthorized account access and drainage).

### `isFlaggedFraud`
* **Type**: Binary (0 or 1)
* **Definition**: Legacy threshold flags. Set to `1` by an automated business rule that flags any single `TRANSFER` exceeding `200,000` currency units.

---

## 2. Engineered Features

These features are designed to amplify anomalous signals for the machine learning model:

### `origin_balance_diff`
* **Formula**: `oldbalanceOrg - newbalanceOrig`
* **Type**: Float
* **Business Meaning**: Measures the exact net cash volume leaving the sender's account. In legitimate transactions, this matches the `amount` (or is negative for Cash-Ins). In fraud cases, this indicates how much the account was cleared out.

### `dest_balance_diff`
* **Formula**: `newbalanceDest - oldbalanceDest`
* **Type**: Float
* **Business Meaning**: Measures the exact net cash volume settled in the recipient's account. Legitimate transfers update destination balances cleanly. A significant discrepancy here (e.g. money entering an account but destination balances showing 0 change) indicates shell account forwarding or immediate cash-out.

### `amount_balance_ratio`
* **Formula**: `amount / (oldbalanceOrg + 1)`
* **Type**: Float
* **Business Meaning**: Computes the relative size of the transaction against the sender's total capital. A value near `1.0` represents a high-risk transaction where the customer attempts to withdraw or transfer almost all of their funds. We add `1` to the denominator to prevent division-by-zero errors for empty accounts.

### `account_drained`
* **Formula**: `(newbalanceOrig == 0) -> 1 else 0`
* **Type**: Binary (0 or 1)
* **Business Meaning**: Specifically flags whether the transaction leaves the sender's balance at exactly zero. Since fraudsters aim to maximize loot per compromise, they routinely empty the account.

### `high_value_txn`
* **Formula**: `(amount > threshold_95th) -> 1 else 0`
* **Type**: Binary (0 or 1)
* **Business Meaning**: A flag identifying transactions with values in the top 5% of all historical amounts. Large-volume movements carry significantly higher risk and are subjected to tighter operational monitoring limits.
