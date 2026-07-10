import logging
from pathlib import Path
import joblib
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit

logger = logging.getLogger("spark-preprocessing")

# Paths to artifacts
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCALER_PATH = PROJECT_ROOT / "deployment" / "models" / "scaler_v1.pkl"

def load_scaler():
    """Load the joblib scaler if it exists, otherwise return None."""
    if SCALER_PATH.exists():
        try:
            return joblib.load(SCALER_PATH)
        except Exception as e:
            logger.error("Failed to load scaler from %s: %s", SCALER_PATH, e)
    return None

def preprocess_stream(df: DataFrame) -> DataFrame:
    """Filter invalid records and clean the streaming data.

    Step 3: Remove invalid records
    """
    logger.info("Applying streaming validation and filtering...")
    # Filter out null values for amount and critical balance columns,
    # and restrict to valid transaction types.
    valid_types = ["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]
    
    clean_df = df.filter(
        (col("amount").isNotNull()) & (col("amount") >= 0) &
        (col("oldbalanceOrg").isNotNull()) &
        (col("newbalanceOrig").isNotNull()) &
        (col("oldbalanceDest").isNotNull()) &
        (col("newbalanceDest").isNotNull()) &
        (col("type").isin(valid_types))
    )
    return clean_df

def engineer_features(df: DataFrame, high_value_threshold: float = 200000.0) -> DataFrame:
    """Recreate engineered features inside Spark.

    Step 4: Recreate Engineered Features
    - origin_balance_diff
    - dest_balance_diff
    - amount_balance_ratio
    - account_drained
    - high_value_txn
    """
    logger.info("Engineering stream features...")
    
    # 1. Origin Balance Difference (Sender money movement)
    df = df.withColumn("origin_balance_diff", col("oldbalanceOrg") - col("newbalanceOrig"))
    
    # 2. Destination Balance Difference (Receiver money movement)
    df = df.withColumn("dest_balance_diff", col("newbalanceDest") - col("oldbalanceDest"))
    
    # 3. Amount to Balance Ratio
    df = df.withColumn("amount_balance_ratio", col("amount") / (col("oldbalanceOrg") + 1.0))
    
    # 4. Account Drained Flag
    df = df.withColumn("account_drained", when(col("newbalanceOrig") == 0.0, 1).otherwise(0))
    
    # 5. High Value Transaction Flag
    df = df.withColumn("high_value_txn", when(col("amount") > high_value_threshold, 1).otherwise(0))
    
    return df

def one_hot_encode_type(df: DataFrame) -> DataFrame:
    """One-hot encode transaction type to match training features.

    Step 5: One-Hot Encode Transaction Type
    """
    logger.info("One-hot encoding categorical transaction type...")
    
    df = df.withColumn("type_CASH_OUT", when(col("type") == "CASH_OUT", 1).otherwise(0))
    df = df.withColumn("type_DEBIT", when(col("type") == "DEBIT", 1).otherwise(0))
    df = df.withColumn("type_PAYMENT", when(col("type") == "PAYMENT", 1).otherwise(0))
    df = df.withColumn("type_TRANSFER", when(col("type") == "TRANSFER", 1).otherwise(0))
    
    return df

def scale_features(df: DataFrame) -> DataFrame:
    """Load the StandardScaler and mathematically scale the features.

    Step 7: Apply Scaler
    """
    scaler = load_scaler()
    if not scaler:
        logger.warning("Scaler not found at %s. Skipping scaling.", SCALER_PATH)
        return df
        
    logger.info("Applying StandardScaler scaling mathematically in PySpark...")
    
    # Feature columns in exact order
    feature_cols = [
        "step", "amount", "oldbalanceOrg", "newbalanceOrig",
        "oldbalanceDest", "newbalanceDest",
        "type_CASH_OUT", "type_DEBIT", "type_PAYMENT", "type_TRANSFER"
    ]
    
    means = scaler.mean_
    scales = scaler.scale_
    
    # Apply (val - mean) / scale for each feature column
    for i, field in enumerate(feature_cols):
        mean_val = float(means[i])
        scale_val = float(scales[i])
        
        scaled_col_name = f"scaled_{field}"
        # Prevent division by zero
        if scale_val == 0.0:
            scale_val = 1.0
            
        df = df.withColumn(scaled_col_name, (col(field) - lit(mean_val)) / lit(scale_val))
        
    return df
