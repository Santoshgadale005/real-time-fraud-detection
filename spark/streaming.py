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
from spark.ml_predictor import MLPredictor
from database.mongodb import MongoDBClient
from monitoring.health import HealthMonitor

logger = logging.getLogger("spark-streaming-pipeline")

# Checkpoint Path
CHECKPOINT_DIR = PROJECT_ROOT / "checkpoints" / "streaming"

# ── Global singletons ────────────────────────────────────────────────────────
health = HealthMonitor()
health.register("kafka")
health.register("spark")
health.register("model")
health.register("mongodb")

# Initialize ML Predictor
try:
    predictor = MLPredictor()
    health.mark_healthy("model", "Isolation Forest loaded")
    logger.info("✅ MLPredictor initialized successfully.")
except Exception as e:
    logger.critical("FATAL — Failed to load ML model: %s", e, exc_info=True)
    health.mark_unhealthy("model", str(e))
    predictor = None

# Initialize MongoDB Client
try:
    db_client = MongoDBClient()
    health.mark_healthy("mongodb", "Connected")
    logger.info("✅ MongoDBClient initialized successfully.")
except Exception as e:
    logger.error("MongoDB initialization failed (non-fatal): %s", e)
    health.mark_unhealthy("mongodb", str(e))
    db_client = None


def process_micro_batch(df: DataFrame, batch_id: int) -> None:
    """Callback for each streaming micro-batch — runs ML inference and MongoDB routing.

    Production hardening (Day 21):
      - Tracks KPIs via health.kpis
      - Logs rejected/invalid records
      - Handles model and database errors without crashing the stream
    """
    t_start = time.perf_counter()
    health.kpis.total_batches += 1
    logger.info("──── Micro-batch #%d START ────", batch_id)

    count = df.count()
    if count == 0:
        logger.info("Micro-batch #%d is empty — skipping.", batch_id)
        return

    try:
        # ── Convert to Pandas ────────────────────────────────────────────────
        pandas_df = df.toPandas()
        health.kpis.total_transactions += len(pandas_df)

        # ── Step 4 (Day 21): Log rejected records ───────────────────────────
        initial_count = len(pandas_df)
        # Drop rows with nulls in critical columns
        critical_cols = ["step", "amount", "oldbalanceOrg", "newbalanceOrig",
                         "oldbalanceDest", "newbalanceDest"]
        available_critical = [c for c in critical_cols if c in pandas_df.columns]
        clean_df = pandas_df.dropna(subset=available_critical)
        rejected = initial_count - len(clean_df)
        if rejected > 0:
            logger.warning("Batch #%d: Rejected %d invalid transactions (null critical fields).", batch_id, rejected)

        if clean_df.empty:
            logger.warning("Batch #%d: All transactions rejected — no valid records.", batch_id)
            return

        # ── Step 5 (Day 21): Handle model errors ────────────────────────────
        if predictor is None:
            logger.error("Batch #%d: Skipping ML inference — model not loaded.", batch_id)
            health.kpis.total_errors += 1
            return

        try:
            predicted_df = predictor.predict_pandas(clean_df)
        except Exception as e:
            logger.error("Batch #%d: ML prediction FAILED: %s", batch_id, e, exc_info=True)
            health.kpis.total_errors += 1
            health.mark_unhealthy("model", f"Prediction error: {e}")
            return

        health.mark_healthy("model", "Predictions OK")

        # ── Separate fraud and normal ────────────────────────────────────────
        fraud_df = predicted_df[predicted_df["prediction"] == 1].copy()
        normal_count = len(predicted_df) - len(fraud_df)
        health.kpis.total_fraud_alerts += len(fraud_df)
        health.kpis.total_normal += normal_count

        t_predict_ms = (time.perf_counter() - t_start) * 1000

        # ── Step 8 (Day 21): Log batch performance ──────────────────────────
        logger.info(
            "Batch #%d INFERENCE | rows=%d | fraud=%d | normal=%d | latency=%.1f ms",
            batch_id, len(predicted_df), len(fraud_df), normal_count, t_predict_ms,
        )

        # ── Console display ──────────────────────────────────────────────────
        if not predicted_df.empty:
            display_cols = [c for c in ["transaction_id", "type", "amount", "prediction", "anomaly_score"]
                           if c in predicted_df.columns]
            print(f"\n═══ BATCH #{batch_id} LIVE PREDICTIONS ({len(predicted_df)} txns) ═══")
            print(predicted_df[display_cols].head(15).to_string(index=False))
            if len(predicted_df) > 15:
                print(f"... and {len(predicted_df) - 15} more.")
            print("═" * 55 + "\n")

        # ── Step 6 (Day 21): Handle MongoDB failures ────────────────────────
        if not fraud_df.empty:
            alerts = []
            for _, row in fraud_df.iterrows():
                score = row["anomaly_score"]
                severity = "HIGH" if score < -0.8 else ("MEDIUM" if score < -0.4 else "LOW")

                alert_doc = {
                    "transaction_id": str(row.get("transaction_id", "unknown")),
                    "event_time": str(row.get("event_time", "")),
                    "detection_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "type": str(row.get("type", "")),
                    "amount": float(row["amount"]),
                    "oldbalanceOrg": float(row.get("oldbalanceOrg", 0)),
                    "newbalanceOrig": float(row.get("newbalanceOrig", 0)),
                    "oldbalanceDest": float(row.get("oldbalanceDest", 0)),
                    "newbalanceDest": float(row.get("newbalanceDest", 0)),
                    "prediction": int(row["prediction"]),
                    "anomaly_score": float(row["anomaly_score"]),
                    "severity": severity,
                }
                alerts.append(alert_doc)

            if db_client is not None:
                try:
                    inserted = db_client.insert_alerts_batch(alerts)
                    health.kpis.total_mongo_writes += inserted
                    health.mark_healthy("mongodb", f"Wrote {inserted} alerts")
                    logger.info("Batch #%d: Persisted %d/%d fraud alerts to MongoDB.", batch_id, inserted, len(alerts))
                except Exception as e:
                    logger.error("Batch #%d: MongoDB write FAILED: %s", batch_id, e, exc_info=True)
                    health.kpis.total_errors += 1
                    health.mark_unhealthy("mongodb", f"Write error: {e}")
            else:
                logger.warning("Batch #%d: MongoDB unavailable — %d fraud alerts NOT persisted.", batch_id, len(alerts))

    except Exception as e:
        logger.error("Batch #%d: UNEXPECTED error: %s", batch_id, e, exc_info=True)
        health.kpis.total_errors += 1

    finally:
        elapsed = (time.perf_counter() - t_start) * 1000
        logger.info("──── Micro-batch #%d END (%.1f ms) ────", batch_id, elapsed)


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
    encoded_df = one_hot_encode_type(engineered_df)
    processed_df = scale_features(encoded_df)
    assert_schema(processed_df, stage="production-pipeline")

    # ── 5. Select output columns ─────────────────────────────────────────────
    output_cols = [
        "transaction_id", "event_time", "type", "amount",
        "step", "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest",
        "type_CASH_OUT", "type_DEBIT", "type_PAYMENT", "type_TRANSFER",
    ]
    available = set(processed_df.columns)
    final_df = processed_df.select([c for c in output_cols if c in available])

    # ── 6. Checkpoint ────────────────────────────────────────────────────────
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

    # ── 7. Start streaming with foreachBatch ─────────────────────────────────
    query = (
        final_df.writeStream
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", str(CHECKPOINT_DIR))
        .trigger(processingTime="5 seconds")
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
