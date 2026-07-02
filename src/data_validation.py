import pandas as pd


def validate_dataset(df):
    print("\n========== DATASET VALIDATION ==========")

    # Shape
    print(f"\nRows: {df.shape[0]}")
    print(f"Columns: {df.shape[1]}")

    # Column names
    print("\nColumns:")
    print(df.columns.tolist())

    # Data types
    print("\nData Types:")
    print(df.dtypes)

    # Missing values
    print("\nMissing Values:")
    print(df.isnull().sum())

    # Duplicate rows
    print("\nDuplicate Rows:")
    print(df.duplicated().sum())

    print("\n========== VALIDATION COMPLETED ==========")