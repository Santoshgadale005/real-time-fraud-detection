import os
import numpy as np
import pandas as pd

# Set random seed for reproducibility
np.random.seed(42)

def generate_paysim_sample(output_path, num_records=100000, fraud_rate=0.0013):
    print(f"Generating synthetic PaySim dataset with {num_records} rows...")
    
    # 1. Generate core components
    steps = np.random.randint(1, 744, size=num_records)
    
    # Transaction types and their probabilities
    types_pool = ['CASH_OUT', 'PAYMENT', 'CASH_IN', 'TRANSFER', 'DEBIT']
    types_probs = [0.35, 0.33, 0.22, 0.09, 0.01]
    types = np.random.choice(types_pool, size=num_records, p=types_probs)
    
    # Generate log-normal transaction amounts
    # Scale to typical financial transaction volumes
    amounts = np.random.lognormal(mean=9.5, sigma=1.5, size=num_records).round(2)
    
    # Generate account names
    orig_ids = np.random.randint(1000000, 9999999, size=num_records)
    dest_ids = np.random.randint(1000000, 9999999, size=num_records)
    
    name_orig = [f"C{oid}" for oid in orig_ids]
    name_dest = []
    for i, t in enumerate(types):
        if t == 'PAYMENT':
            name_dest.append(f"M{dest_ids[i]}")  # Merchants for payments
        else:
            name_dest.append(f"C{dest_ids[i]}")  # Customers for others
            
    # Generate starting balances
    old_balance_org = np.random.lognormal(mean=11.0, sigma=2.0, size=num_records).round(2)
    
    # Initialize target columns
    is_fraud = np.zeros(num_records, dtype=int)
    is_flagged_fraud = np.zeros(num_records, dtype=int)
    
    # Calculate target fraud count
    target_fraud_count = int(num_records * fraud_rate)
    
    # Fraud only happens in TRANSFER and CASH_OUT
    fraud_eligible_idx = np.where((types == 'TRANSFER') | (types == 'CASH_OUT'))[0]
    fraud_indices = np.random.choice(fraud_eligible_idx, size=target_fraud_count, replace=False)
    
    is_fraud[fraud_indices] = 1
    
    # Process origin balances
    new_balance_orig = np.zeros(num_records)
    for i in range(num_records):
        if is_fraud[i] == 1:
            # Fraud behavior: empty the account
            # Make transaction amount equal to the entire old balance
            amounts[i] = old_balance_org[i] if old_balance_org[i] > 100 else amounts[i]
            new_balance_orig[i] = 0.0
            
            # Highlight flag rule (PaySim flags TRANSFER > 200,000)
            if types[i] == 'TRANSFER' and amounts[i] > 200000:
                is_flagged_fraud[i] = 1
        else:
            # Normal behavior
            if types[i] in ['CASH_OUT', 'TRANSFER', 'PAYMENT']:
                new_balance_orig[i] = max(0.0, old_balance_org[i] - amounts[i])
            elif types[i] in ['CASH_IN']:
                new_balance_orig[i] = old_balance_org[i] + amounts[i]
            else: # DEBIT
                new_balance_orig[i] = max(0.0, old_balance_org[i] - amounts[i])
                
    new_balance_orig = new_balance_orig.round(2)
    
    # Process destination balances
    old_balance_dest = np.random.lognormal(mean=10.5, sigma=2.2, size=num_records).round(2)
    new_balance_dest = np.zeros(num_records)
    
    for i in range(num_records):
        if name_dest[i].startswith('M'):
            # Merchants have no balance track record in PaySim
            old_balance_dest[i] = 0.0
            new_balance_dest[i] = 0.0
        else:
            if is_fraud[i] == 1:
                # Anomaly: Fraudulent transfers often show zero destination balance changes in PaySim
                if np.random.rand() > 0.5:
                    old_balance_dest[i] = 0.0
                    new_balance_dest[i] = 0.0
                else:
                    new_balance_dest[i] = old_balance_dest[i] + amounts[i]
            else:
                if types[i] in ['CASH_OUT', 'TRANSFER']:
                    new_balance_dest[i] = old_balance_dest[i] + amounts[i]
                elif types[i] in ['CASH_IN']:
                    new_balance_dest[i] = max(0.0, old_balance_dest[i] - amounts[i])
                else: # DEBIT / PAYMENT (handled above)
                    new_balance_dest[i] = old_balance_dest[i]
                    
    old_balance_dest = old_balance_dest.round(2)
    new_balance_dest = new_balance_dest.round(2)
    
    # Create DataFrame
    df = pd.DataFrame({
        'step': steps,
        'type': types,
        'amount': amounts,
        'nameOrig': name_orig,
        'oldbalanceOrg': old_balance_org,
        'newbalanceOrig': new_balance_orig,
        'nameDest': name_dest,
        'oldbalanceDest': old_balance_dest,
        'newbalanceDest': new_balance_dest,
        'isFraud': is_fraud,
        'isFlaggedFraud': is_flagged_fraud
    })
    
    # Save to CSV
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Dataset successfully created and saved to {output_path}!")
    print(f"Shape: {df.shape}")
    print(f"Fraud count: {df['isFraud'].sum()} ({df['isFraud'].mean()*100:.4f}%)")

if __name__ == '__main__':
    generate_paysim_sample('data/paysim.csv')
