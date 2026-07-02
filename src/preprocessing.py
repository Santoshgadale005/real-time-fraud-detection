import os
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from data_validation import validate_dataset


def load_data():
    """
    Load the raw PaySim dataset.
    """
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_path = os.path.join(project_root, "data", "raw", "paysim.csv")

    df = pd.read_csv(csv_path)

    print("=" * 60)
    print("Dataset Loaded Successfully")
    print("=" * 60)

    return df


def remove_duplicates(df):
    """
    Remove duplicate records.
    """
    before = len(df)

    df = df.drop_duplicates()

    after = len(df)

    print(f"Duplicate Rows Removed : {before - after}")

    return df


def handle_missing_values(df):
    """
    Handle missing values.
    """
    missing = df.isnull().sum().sum()

    if missing == 0:
        print("No Missing Values Found")
    else:
        df = df.fillna(method="ffill")
        print(f"Handled {missing} Missing Values")

    return df


def feature_engineering(df):
    """
    Perform feature engineering.
    """

    print("\nStarting Feature Engineering...")

    # Encode transaction type
    encoder = LabelEncoder()

    df["type"] = encoder.fit_transform(df["type"])

    print("Encoded 'type' column")

    # Drop customer ID columns
    df.drop(["nameOrig", "nameDest"], axis=1, inplace=True)

    print("Dropped 'nameOrig' and 'nameDest'")

    print("Feature Engineering Completed")

    return df


def save_processed_data(df):
    """
    Save processed dataset.
    """

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    processed_folder = os.path.join(project_root, "data", "processed")

    os.makedirs(processed_folder, exist_ok=True)

    output_file = os.path.join(
        processed_folder,
        "processed_paysim.csv"
    )

    df.to_csv(output_file, index=False)

    print("\nProcessed Dataset Saved Successfully")
    print(output_file)


if __name__ == "__main__":

    print("\nStarting Preprocessing Pipeline...\n")

    # Step 1
    df = load_data()

    # Step 2
    validate_dataset(df)

    # Step 3
    df = remove_duplicates(df)

    # Step 4
    df = handle_missing_values(df)

    # Step 5
    df = feature_engineering(df)

    # Step 6
    save_processed_data(df)

    print("\nPreprocessing Pipeline Completed Successfully")

    print("\nProcessed Dataset Preview:\n")

    print(df.head())