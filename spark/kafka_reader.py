import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json
from spark.schema import TRANSACTION_SCHEMA

logger = logging.getLogger("spark-kafka-reader")

def read_kafka_stream(
    spark: SparkSession, 
    bootstrap_servers: str, 
    topic: str,
    starting_offsets: str = "latest"
) -> DataFrame:
    """Read a streaming dataframe from a Kafka topic.

    Steps 5 & 6: Connect Spark to Kafka & Read Kafka Stream
    """
    logger.info("Connecting to Kafka bootstrap servers: %s on topic: %s", bootstrap_servers, topic)
    
    # Read stream from Kafka
    raw_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", starting_offsets) \
        .load()
        
    logger.info("Kafka streaming connection established.")
    
    # Convert binary value to String, parse JSON schema, and select fields
    # Step 7 & 9: Convert Binary Messages & Parse Incoming JSON
    parsed_stream_df = raw_stream_df \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), TRANSACTION_SCHEMA).alias("data")) \
        .select("data.*")
        
    return parsed_stream_df
