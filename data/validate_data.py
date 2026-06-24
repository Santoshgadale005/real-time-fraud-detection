import pandas as pd

df = pd.read_csv("data/raw/paysim.csv")

print("=" * 50)
print("DATA VALIDATION REPORT")
print("=" * 50)

print(f"\nDataset Shape: {df.shape}")

print("\nMissing Values:")
print(df.isnull().sum())

print(f"\nDuplicate Rows: {df.duplicated().sum()}")

print("\nData Types:")
print(df.dtypes)

print("\nFraud Distribution:")
print(df["isFraud"].value_counts())

print("\nFraud Percentage:")
print(round(df["isFraud"].mean() * 100, 4), "%")