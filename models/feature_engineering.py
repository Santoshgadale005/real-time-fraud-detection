import pandas as pd
import numpy as np

def engineer_features(df, high_value_threshold=None):
    """
    Applies the standardized feature engineering pipeline to the transaction DataFrame.
    
    Parameters:
    - df (pd.DataFrame): Raw transaction DataFrame containing columns:
      ['amount', 'oldbalanceOrg', 'newbalanceOrig', 'oldbalanceDest', 'newbalanceDest']
    - high_value_threshold (float): Threshold to classify a transaction as high-value.
      If None, it will be computed as the 95th percentile of the 'amount' column in the input DataFrame.
      
    Returns:
    - pd.DataFrame: Processed DataFrame containing both original and engineered features.
    """
    # Create a copy to prevent SettingWithCopyWarning
    processed_df = df.copy()
    
    # 1. Origin Balance Difference (Sender money movement)
    processed_df['origin_balance_diff'] = processed_df['oldbalanceOrg'] - processed_df['newbalanceOrig']
    
    # 2. Destination Balance Difference (Receiver money movement)
    processed_df['dest_balance_diff'] = processed_df['newbalanceDest'] - processed_df['oldbalanceDest']
    
    # 3. Amount to Balance Ratio (Risk ratio of amount vs. starting balance)
    processed_df['amount_balance_ratio'] = processed_df['amount'] / (processed_df['oldbalanceOrg'] + 1)
    
    # 4. Account Drained Flag (Checks if transaction leaves sender with exactly $0)
    processed_df['account_drained'] = (processed_df['newbalanceOrig'] == 0).astype(int)
    
    # 5. High-Value Transaction Flag (Scrutiny flag based on transaction amount threshold)
    if high_value_threshold is None:
        high_value_threshold = processed_df['amount'].quantile(0.95)
        
    processed_df['high_value_txn'] = (processed_df['amount'] > high_value_threshold).astype(int)
    
    return processed_df

if __name__ == '__main__':
    # Test script execution
    print("Testing feature engineering script...")
    try:
        test_df = pd.read_csv('data/paysim.csv')
        processed = engineer_features(test_df)
        print("Feature engineering pipeline test: PASSED!")
        print(f"Columns in processed data: {list(processed.columns)}")
        print(f"Engineered columns stats:\n{processed[['origin_balance_diff', 'dest_balance_diff', 'amount_balance_ratio', 'account_drained', 'high_value_txn']].describe()}")
    except Exception as e:
        print(f"Feature engineering pipeline test: FAILED with error: {e}")
