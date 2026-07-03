import os
import joblib
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix
)
project_root = os.path.dirname(
    os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
    )
)

dataset_path = os.path.join(
    project_root,
    "data",
    "processed",
    "processed_paysim.csv"
)

df = pd.read_csv(dataset_path)

print("Processed dataset loaded successfully.")
print(df.head())
# Split features and target
X = df.drop("isFraud", axis=1)
y = df["isFraud"]

print("\nFeatures Shape :", X.shape)
print("Target Shape   :", y.shape)

# Train-Test Split
X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=42,
    stratify=y
)

print("\nTrain-Test Split Completed")
print("Training Samples :", len(X_train))
print("Testing Samples  :", len(X_test))

# -----------------------------
# Model Training
# -----------------------------
model = RandomForestClassifier(
    n_estimators=50,
    random_state=42,
    n_jobs=-1
)

print("\nTraining Random Forest Model...")

model.fit(X_train, y_train)

print("Model Training Completed!")
# -----------------------------
# Prediction
# -----------------------------
print("\nMaking Predictions...")

y_pred = model.predict(X_test)

print("Prediction Completed!")

# -----------------------------
# Model Evaluation
# -----------------------------
accuracy = accuracy_score(y_test, y_pred)

print(f"\nAccuracy : {accuracy:.4f}")

print("\nClassification Report:\n")

report = classification_report(y_test, y_pred)

print(report)

print("\nConfusion Matrix:")

print(confusion_matrix(y_test, y_pred))
# -----------------------------
# Save Model
# -----------------------------
model_path = os.path.join(
    project_root,
    "models",
    "fraud_model.pkl"
)

joblib.dump(model, model_path)

print("\nModel Saved Successfully!")

# -----------------------------
# Save Report
# -----------------------------
report_path = os.path.join(
    project_root,
    "reports",
    "classification_report.txt"
)

with open(report_path, "w") as file:
    file.write(report)

print("Classification Report Saved!")

