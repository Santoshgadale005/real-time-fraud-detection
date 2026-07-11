"""spark/streaming.py — Fault-Tolerant Structured Streaming Pipeline (Day 17).

Pipeline:
    Kafka → raw binary → JSON parse → filter → feature engineering
         → one-hot encode → scale → console sink (append mode)

Day 17 additions:
    - Watermark on `timestamp` column for late-data tolerance
    - Dedicated checkpoint directory per query (checkpoints/streaming/)
    - FraudStreamingListener attached to SparkSession for per-batch metrics
    - Schema validation at every pipeline stage via assert_schema()
    - Structured logging: STARTED / BATCH_COMPLETE / SHUTDOWN
    - Graceful KeyboardInterrupt handling
"""

import os
import sys
import logging
import time
from pathlib import Path

# Allow imports from project root
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

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

# ---------------------------------------------------------------------------
# Checkpoint directory
# ---------------------------------------------------------------------------
CHECKPOINT_DIR = PROJECT_ROOT / "checkpoints" / "streaming"


def main() -> None:
    logger.info("=" * 70)
    logger.info("BATCH STARTED — Spark Structured Streaming Fraud Detection Pipeline")
    logger.info("=" * 70)

    # ── 1. Create SparkSession ───────────────────────────────────────────────
    spark = get_spark_session("FraudDetectionPipeline-Day17")

    # ── Attach streaming query listener (Day 17 — Step 6 & 9) ───────────────
    listener = FraudStreamingListener()
    spark.streams.addListener(listener)
    logger.info("FraudStreamingListener attached to SparkSession.")

    # ── 2. Read raw Kafka stream ─────────────────────────────────────────────
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", KAFKA_BOOTSTRAP_SERVERS)
    topic             = os.getenv("KAFKA_TRANSACTIONS_TOPIC", TRANSACTIONS_TOPIC)
    logger.info("Reading stream | topic=%s | broker=%s", topic, bootstrap_servers)

    raw_df = read_kafka_stream(spark, bootstrap_servers, topic, starting_offsets="latest")

    # ── 3. Add Watermark on event timestamp (Day 17 — Step 4 & 5) ───────────
    # The producer encodes a UTC ISO-8601 `timestamp` string field.
    # We cast it to a Spark TimestampType and apply a 10-second watermark so
    # that late-arriving data is still considered within that window.
    from pyspark.sql.functions import col, to_timestamp

    watermarked_df = (
        raw_df
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .withWatermark("event_time", "10 seconds")
    )
    logger.info("Watermark configured: 10 seconds on 'event_time'.")

    # ── 4. Filter invalid records (Step 8) ───────────────────────────────────
    clean_df = preprocess_stream(watermarked_df)
    logger.info("Invalid record filter applied.")

    # ── 5. Feature engineering ───────────────────────────────────────────────
    engineered_df = engineer_features(clean_df)
    encoded_df    = one_hot_encode_type(engineered_df)
    processed_df  = scale_features(encoded_df)

    # ── 6. Schema validation (Day 17 — Step 13) ──────────────────────────────
    assert_schema(processed_df, stage="post-preprocessing")

    # ── 7. Select output columns (feature set + metadata) ────────────────────
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
    final_df  = processed_df.select([c for c in output_cols if c in available])

    # ── 8. Create checkpoint directory (Day 17 — Step 3) ─────────────────────
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Checkpoint directory: %s", CHECKPOINT_DIR)

    # ── 9. Write to console (append mode, 5-second trigger) ──────────────────
    query = (
        final_df.writeStream
        .format("console")
        .outputMode("append")                                 # Step 7
        .option("checkpointLocation", str(CHECKPOINT_DIR))   # Step 3
        .option("truncate", "false")
        .trigger(processingTime="5 seconds")                  # micro-batch
        .start()
    )

    logger.info("Streaming query started | outputMode=append | trigger=5s")
    print_query_status(query, label="initial")

    # ── 10. Await termination with periodic status logs ───────────────────────
    try:
        while query.isActive:
            time.sleep(30)
            print_query_status(query, label="heartbeat")
    except KeyboardInterrupt:
        logger.info("⚠️  Shutdown signal received — stopping streaming query...")
        query.stop()
        logger.info("Streaming query stopped cleanly.")
    except Exception as exc:
        logger.error("Unexpected error in streaming loop: %s", exc, exc_info=True)
        query.stop()
    finally:
        spark.streams.removeListener(listener)
        logger.info("BATCH COMPLETED — Streaming pipeline shut down.")


if __name__ == "__main__":
    main()
