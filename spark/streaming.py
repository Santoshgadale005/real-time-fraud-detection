import os
import sys
import logging
from pathlib import Path

# Allow imports from project root
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from config.kafka_config import KAFKA_BOOTSTRAP_SERVERS, TRANSACTIONS_TOPIC
from spark.utils import get_spark_session
from spark.kafka_reader import read_kafka_stream
from spark.preprocessing import preprocess_stream, engineer_features, one_hot_encode_type, scale_features

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("spark-streaming-pipeline")

def main():
    logger.info("Starting Spark Structured Streaming Fraud Detection Pipeline...")
    
    # 1. Create Spark Session
    # Step 4: Create Spark Session
    spark = get_spark_session("FraudDetectionPipeline")
    
    # 2. Connect Spark to Kafka and Read Stream
    # Steps 5 & 6: Connect Spark to Kafka & Read Kafka Stream
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", KAFKA_BOOTSTRAP_SERVERS)
    topic = os.getenv("KAFKA_TRANSACTIONS_TOPIC", TRANSACTIONS_TOPIC)
    
    # Read stream (starting from earliest to consume existing messages if any)
    logger.info("Reading stream from topic: %s", topic)
    raw_df = read_kafka_stream(spark, bootstrap_servers, topic, starting_offsets="earliest")
    
    # 3. Clean and filter invalid records
    # Step 3 (from Day 16): Remove Invalid Records
    clean_df = preprocess_stream(raw_df)
    
    # 4. Feature Engineering
    # Step 4 (from Day 16): Recreate Engineered Features
    engineered_df = engineer_features(clean_df)
    
    # 5. One-hot encoding categorical variables
    # Step 5 (from Day 16): One-Hot Encode Transaction Type
    encoded_df = one_hot_encode_type(engineered_df)
    
    # 6. Apply Standard Scaler
    # Step 7 (from Day 16): Apply Scaler
    processed_df = scale_features(encoded_df)
    
    # 7. Write to Console stream
    # Step 10 (from Day 15) & Step 9 (from Day 16): Display Stream & Output to Console
    checkpoint_dir = PROJECT_ROOT / "checkpoints"
    checkpoint_dir.mkdir(exist_ok=True)
    
    # Set up streaming query
    # Step 12: Configure Checkpointing
    query = processed_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("checkpointLocation", str(checkpoint_dir)) \
        .trigger(processingTime="5 seconds") \
        .start()
        
    logger.info("Streaming query started. Monitoring console for incoming batches...")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Shutdown signal received. Stopping streaming query...")
        query.stop()
    except Exception as e:
        logger.error("Error in streaming query: %s", e)
        query.stop()

if __name__ == "__main__":
    main()
