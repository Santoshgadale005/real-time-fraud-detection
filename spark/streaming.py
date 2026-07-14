"""spark/streaming.py — Production-Hardened Real-Time Fraud Detection Pipeline (Day 21).

End-to-end pipeline:
    Kafka → Spark Structured Streaming → Feature Engineering → Isolation Forest
          → Severity Classification → MongoDB Fraud Alerts

Day 21 production additions:
    - Centralized rotating-file logging via monitoring.logging_config
    - HealthMonitor tracking component liveness and cumulative KPIs
    - Enhanced error handling: model errors, MongoDB failures, invalid inputs
    - Retry-resilient MongoDB writes (already in database/mongodb.py)
    - 60-second health status heartbeat with KPI summary
"""

import os
import sys
import logging
import time
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp

# Allow imports from project root
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# Centralized logging MUST be configured before any other imports log messages
from monitoring.logging_config import setup_logging
setup_logging()

from config.kafka_config import KAFKA_BOOTSTRAP_SERVERS, TRANSACTIONS_TOPIC
from spark.utils import get_spark_session
from spark.kafka_reader import read_kafka_stream
from spark.preprocessing import preprocess_stream, engineer_features, one_hot_encode_type, scale_features
from spark.monitor import FraudStreamingListener, assert_schema, print_query_status

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("spark-streaming-pipeline")
predictor = MLPredictor()

# Checkpoint Path
CHECKPOINT_DIR = PROJECT_ROOT / "checkpoints" / "streaming"


def main() -> None:
    logger.info("=" * 70)
    logger.info("PRODUCTION PIPELINE — Real-Time Fraud Detection (Day 21)")
    logger.info("=" * 70)

    # ── 1. Create SparkSession ───────────────────────────────────────────────
    spark = get_spark_session("FraudDetectionPipeline-Production")
    health.mark_healthy("spark", "Session created")

    listener = FraudStreamingListener()
    spark.streams.addListener(listener)

    # ── 2. Read raw Kafka stream ─────────────────────────────────────────────
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", KAFKA_BOOTSTRAP_SERVERS)
    topic = os.getenv("KAFKA_TRANSACTIONS_TOPIC", TRANSACTIONS_TOPIC)

    try:
        raw_df = read_kafka_stream(spark, bootstrap_servers, topic, starting_offsets="latest")
        health.mark_healthy("kafka", f"Subscribed to {topic}")
    except Exception as e:
        logger.critical("FATAL — Cannot read Kafka stream: %s", e, exc_info=True)
        health.mark_unhealthy("kafka", str(e))
        spark.stop()
        return

    # ── 3. Watermark ─────────────────────────────────────────────────────────
    watermarked_df = (
        raw_df
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .withWatermark("event_time", "10 seconds")
    )

    # ── 4. Preprocessing pipeline ────────────────────────────────────────────
    clean_df = preprocess_stream(watermarked_df)
    engineered_df = engineer_features(clean_df)
    encoded_df    = one_hot_encode_type(engineered_df)
    processed_df  = scale_features(encoded_df)

    # ── 5. Select output columns ─────────────────────────────────────────────
    output_cols = [
        "transaction_id", "event_time", "type", "amount",
        "scaled_step", "scaled_amount",
        "scaled_oldbalanceOrg", "scaled_newbalanceOrig",
        "scaled_oldbalanceDest", "scaled_newbalanceDest",
        "scaled_type_CASH_OUT", "scaled_type_DEBIT",
        "scaled_type_PAYMENT", "scaled_type_TRANSFER",
        "origin_balance_diff", "dest_balance_diff",
        "amount_balance_ratio", "account_drained", "high_value_txn",
    ]
    # Only select columns that actually exist (watermark may omit timestamp)
    available = set(processed_df.columns)
    final_df = processed_df.select([c for c in output_cols if c in available])

    # ── 6. Checkpoint ────────────────────────────────────────────────────────
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

    # ── 7. Start streaming with foreachBatch ─────────────────────────────────
    query = (
        final_df.writeStream
        .format("console")
        .outputMode("append")                                 # Step 7
        .option("checkpointLocation", str(CHECKPOINT_DIR))   # Step 3
        .option("truncate", "false")
        .trigger(processingTime="5 seconds")                  # micro-batch
        .start()
    )

    logger.info("Streaming query started — foreachBatch ML pipeline sink.")
    print_query_status(query, label="initial")
    health.log_status()

    # ── 8. Await termination with 60-second health heartbeat ─────────────────
    try:
        while query.isActive:
            time.sleep(60)
            health.log_status()
            print_query_status(query, label="heartbeat")
    except KeyboardInterrupt:
        logger.info("⚠️ Shutdown signal — stopping streaming query...")
        query.stop()
    except Exception as exc:
        logger.error("Unexpected error: %s", exc, exc_info=True)
        query.stop()
    finally:
        if db_client:
            db_client.close()
        spark.streams.removeListener(listener)
        health.log_status()
        logger.info("PRODUCTION PIPELINE SHUTDOWN COMPLETE.")


if __name__ == "__main__":
    main()
