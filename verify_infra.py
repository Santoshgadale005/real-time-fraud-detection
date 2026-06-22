import os
import sys
from dotenv import load_dotenv
from pymongo import MongoClient
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Load environment variables
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin123@localhost:27017")
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")

print("=========================================")
print("System Verification Script")
print("=========================================")

# 1. Verify MongoDB Connection
try:
    print(f"Connecting to MongoDB at: {MONGO_URI.split('@')[-1]} ...")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
    # The ismaster command is cheap and does not require auth.
    # We will trigger server selection by pinging or calling admin db.
    client.admin.command('ping')
    print("✅ MongoDB connection successful!")
except Exception as e:
    print(f"❌ MongoDB connection failed: {e}")
    sys.exit(1)

# 2. Verify Kafka Broker Connection
try:
    print(f"Connecting to Kafka Broker at: {KAFKA_SERVER} ...")
    # Attempting to fetch metadata from broker
    consumer = KafkaConsumer(
        bootstrap_servers=[KAFKA_SERVER]
    )
    topics = consumer.topics()
    print("✅ Kafka connection successful!")
    print(f"Existing Topics: {list(topics)}")
except KafkaError as ke:
    print(f"❌ Kafka connection failed: {ke}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Kafka connection failed with error: {e}")
    sys.exit(1)

print("\n🎉 Infrastructure verification PASSED! All services are healthy and reachable.")
print("=========================================")
