# PaySim Dataset Data Dictionary

| Column | Description |
|----------|------------|
| step | Unit of time (1 step = 1 hour) |
| type | Type of transaction |
| amount | Transaction amount |
| nameOrig | Customer initiating transaction |
| oldbalanceOrg | Initial balance before transaction |
| newbalanceOrig | New balance after transaction |
| nameDest | Recipient customer |
| oldbalanceDest | Recipient balance before transaction |
| newbalanceDest | Recipient balance after transaction |
| isFraud | Fraud indicator (0 = No, 1 = Yes) |
| isFlaggedFraud | Flagged suspicious transaction |