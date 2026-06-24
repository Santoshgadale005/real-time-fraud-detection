# src/features/build_features.py

import pandas as pd

df = pd.read_csv("data/raw/paysim.csv")

df["balanceOrigDiff"] = (
    df["oldbalanceOrg"] -
    df["newbalanceOrig"]
)

df["balanceDestDiff"] = (
    df["newbalanceDest"] -
    df["oldbalanceDest"]
)

df["amount_to_balance_ratio"] = (
    df["amount"] /
    (df["oldbalanceOrg"] + 1)
)

print(df.head())

from sklearn.model_selection import train_test_split

X = df.drop("isFraud", axis=1)
y = df["isFraud"]

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=42
)

train_df = pd.concat([X_train, y_train], axis=1)
test_df = pd.concat([X_test, y_test], axis=1)

train_df.to_csv(
    "data/processed/train.csv",
    index=False
)

test_df.to_csv(
    "data/processed/test.csv",
    index=False
)

print("Train/Test files created")