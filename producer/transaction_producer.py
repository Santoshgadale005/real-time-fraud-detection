from kafka import KafkaProducer
import json
import pandas as pd
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Load only a sample for testing
df = pd.read_csv("data/raw/paysim.csv").sample(n=1000, random_state=42)

print("Starting Producer...")

for index, row in df.iterrows():
    producer.send("transactions", row.to_dict())
    print(f"Sent Transaction {index}")
    time.sleep(0.5)   # Send one message every 0.5 seconds

producer.flush()

print("All Transactions Sent!")