import os
import sys
import math
from pathlib import Path
import joblib
import pandas as pd

# Allow imports from project root
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from spark.utils import get_spark_session
from spark.preprocessing import preprocess_stream, engineer_features, one_hot_encode_type, scale_features

def main():
    print("=" * 70)
    # Step 8: Validate Feature Pipeline (Compare Spark vs. Python outputs)
    print("STEP 8: Feature Pipeline Validation (Spark Preprocessing vs. Python Preprocessing)")
    print("=" * 70)
    
    # Initialize Spark
    spark = get_spark_session("TestPipeline")
    
    # 1. Create a single raw sample transaction
    sample_txn = {
        "step": 1,
        "type": "TRANSFER",
        "amount": 180000.0,
        "nameOrig": "C10001",
        "nameDest": "C10002",
        "oldbalanceOrg": 500000.0,
        "newbalanceOrig": 320000.0,
        "oldbalanceDest": 10000.0,
        "newbalanceDest": 190000.0,
        "isFraud": 0,
        "transaction_id": "tx-12345",
        "timestamp": "2026-07-08T12:00:00.000000+00:00"
    }
    
    # Create Spark DataFrame from sample
    raw_spark_df = spark.createDataFrame([sample_txn])
    
    # Apply Spark preprocessing and feature engineering
    clean_df = preprocess_stream(raw_spark_df)
    engineered_df = engineer_features(clean_df)
    encoded_df = one_hot_encode_type(engineered_df)
    processed_df = scale_features(encoded_df)
    
    # Collect results from Spark
    spark_result = processed_df.collect()[0]
    
    # 2. Run Python preprocessing manually to verify
    scaler = joblib.load(PROJECT_ROOT / "deployment" / "models" / "scaler_v1.pkl")
    
    # Map transaction type to one-hot columns
    type_CASH_OUT = 1 if sample_txn["type"] == "CASH_OUT" else 0
    type_DEBIT = 1 if sample_txn["type"] == "DEBIT" else 0
    type_PAYMENT = 1 if sample_txn["type"] == "PAYMENT" else 0
    type_TRANSFER = 1 if sample_txn["type"] == "TRANSFER" else 0
    
    # Build vector in exact order matching features.json
    raw_vector = [
        sample_txn["step"],
        sample_txn["amount"],
        sample_txn["oldbalanceOrg"],
        sample_txn["newbalanceOrig"],
        sample_txn["oldbalanceDest"],
        sample_txn["newbalanceDest"],
        type_CASH_OUT,
        type_DEBIT,
        type_PAYMENT,
        type_TRANSFER
    ]
    
    # Scale using scikit-learn standard scaler
    python_scaled = scaler.transform([raw_vector])[0]
    
    # 3. Compare outputs side-by-side
    feature_cols = [
        "step", "amount", "oldbalanceOrg", "newbalanceOrig",
        "oldbalanceDest", "newbalanceDest",
        "type_CASH_OUT", "type_DEBIT", "type_PAYMENT", "type_TRANSFER"
    ]
    
    print("\nFeature Comparison Table:")
    print("-" * 105)
    print(f"{'Feature Column':<25} | {'Raw Value':<15} | {'Spark Scaled':<25} | {'Python Scaled':<25} | {'Difference':<10}")
    print("-" * 105)
    
    all_match = True
    for idx, col_name in enumerate(feature_cols):
        raw_val = raw_vector[idx]
        spark_scaled_val = spark_result[f"scaled_{col_name}"]
        python_scaled_val = python_scaled[idx]
        diff = abs(spark_scaled_val - python_scaled_val)
        
        # Mark mismatch if difference > 1e-7 (floating point precision limit)
        status_flag = "MATCH" if diff < 1e-7 else "MISMATCH"
        if status_flag == "MISMATCH":
            all_match = False
            
        print(f"{col_name:<25} | {raw_val:<15} | {spark_scaled_val:<25.6f} | {python_scaled_val:<25.6f} | {diff:<10.2e} ({status_flag})")
        
    print("-" * 105)
    
    # Verify engineered features too
    print("\nEngineered Feature Verification:")
    print(f"  origin_balance_diff  : Spark = {spark_result['origin_balance_diff']:<10.2f} (Expected = {sample_txn['oldbalanceOrg'] - sample_txn['newbalanceOrig']:.2f})")
    print(f"  dest_balance_diff    : Spark = {spark_result['dest_balance_diff']:<10.2f} (Expected = {sample_txn['newbalanceDest'] - sample_txn['oldbalanceDest']:.2f})")
    print(f"  amount_balance_ratio : Spark = {spark_result['amount_balance_ratio']:<10.6f} (Expected = {sample_txn['amount'] / (sample_txn['oldbalanceOrg'] + 1.0):.6f})")
    print(f"  account_drained      : Spark = {spark_result['account_drained']:<10d} (Expected = 0)")
    print(f"  high_value_txn       : Spark = {spark_result['high_value_txn']:<10d} (Expected = 0)")
    
    if all_match:
        print("\n🎉 FEATURE PIPELINE VALIDATION SUCCESSFUL: Spark preprocessing outputs match Python outputs perfectly!")
        spark.stop()
        sys.exit(0)
    else:
        print("\n❌ FEATURE PIPELINE VALIDATION FAILED: Mismatches detected between Spark and Python outputs.")
        spark.stop()
        sys.exit(1)

if __name__ == "__main__":
    main()
