"""spark/streaming.py — Production Real-Time Fraud Detection Pipeline (Day 21).

End-to-end pipeline:
    Kafka → Spark Structured Streaming → Feature Engineering → IsolationForest
          → Severity Classification → MongoDB Fraud Alerts

Production features:
    - Centralized rotating-file logging via monitoring.logging_config
    - HealthMonitor tracking component liveness and cumulative KPIs
    - Enhanced error handling: model errors, MongoDB failures, invalid inputs
    - Retry-resilient MongoDB writes (in database/mongodb.py)
    - 60-second health status heartbeat with KPI summary
"""

import os
import sys
import json
import logging
import time
from pathlib import Path

# Allow imports from project root
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# Centralized logging MUST be configured before any other imports log messages
from monitoring.logging_config import setup_logging
setup_logging()

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from config.kafka_config import KAFKA_BOOTSTRAP_SERVERS, TRANSACTIONS_TOPIC
from spark.monitor import FraudStreamingListener, print_query_status
from spark.ml_predictor import MLPredictor
from database.mongodb import MongoDBClient
from monitoring.health import HealthMonitor

logger = logging.getLogger("spark-streaming-pipeline")

# ── Kafka / Checkpoint settings ─────────────────────────────────────────────
CHECKPOINT_DIR = PROJECT_ROOT / "checkpoints" / "streaming"

# ── Transaction schema ───────────────────────────────────────────────────────
TRANSACTION_SCHEMA = StructType([
    StructField("step",            IntegerType(), True),
    StructField("type",            StringType(),  True),
    StructField("amount",          DoubleType(),  True),
    StructField("nameOrig",        StringType(),  True),
    StructField("oldbalanceOrg",   DoubleType(),  True),
    StructField("newbalanceOrig",  DoubleType(),  True),
    StructField("nameDest",        StringType(),  True),
    StructField("oldbalanceDest",  DoubleType(),  True),
    StructField("newbalanceDest",  DoubleType(),  True),
    StructField("isFraud",         IntegerType(), True),
    StructField("isFlaggedFraud",  IntegerType(), True),
    StructField("transaction_id",  StringType(),  True),
    StructField("timestamp",       StringType(),  True),
])

# ── Global singletons ────────────────────────────────────────────────────────
health = HealthMonitor()
health.register("kafka")
health.register("spark")
health.register("model")
health.register("mongodb")

# Initialize ML Predictor
try:
    predictor = MLPredictor()
    health.mark_healthy("model", "IsolationForest loaded")
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
    """Callback for each streaming micro-batch — runs ML inference and MongoDB routing."""
    t_start = time.perf_counter()
    health.kpis.total_batches += 1
    logger.info("──── Micro-batch #%d START ────", batch_id)

    count = df.count()
    if count == 0:
        logger.info("Micro-batch #%d is empty — skipping.", batch_id)
        return

    try:
        # ── Convert to Pandas ──────────────────────────────────────────────
        pandas_df = df.toPandas()
        health.kpis.total_transactions += len(pandas_df)

        # ── Drop rows with nulls in critical columns ────────────────────────
        critical_cols = ["step", "amount", "oldbalanceOrg", "newbalanceOrig",
                         "oldbalanceDest", "newbalanceDest"]
        available_critical = [c for c in critical_cols if c in pandas_df.columns]
        clean_df = pandas_df.dropna(subset=available_critical)
        rejected = len(pandas_df) - len(clean_df)
        if rejected > 0:
            logger.warning("Batch #%d: Rejected %d invalid transactions.", batch_id, rejected)

        if clean_df.empty:
            logger.warning("Batch #%d: All transactions rejected.", batch_id)
            return

        # ── ML Inference ───────────────────────────────────────────────────
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

        # ── Separate fraud and normal ──────────────────────────────────────
        fraud_df   = predicted_df[predicted_df["prediction"] == 1].copy()
        normal_cnt = len(predicted_df) - len(fraud_df)
        health.kpis.total_fraud_alerts += len(fraud_df)
        health.kpis.total_normal        += normal_cnt

        t_predict_ms = (time.perf_counter() - t_start) * 1000
        logger.info(
            "Batch #%d INFERENCE | rows=%d | fraud=%d | normal=%d | latency=%.1f ms",
            batch_id, len(predicted_df), len(fraud_df), normal_cnt, t_predict_ms,
        )

        # ── Console display ────────────────────────────────────────────────
        if not predicted_df.empty:
            display_cols = [c for c in ["transaction_id", "type", "amount",
                                        "prediction", "anomaly_score"]
                            if c in predicted_df.columns]
            print(f"\n═══ BATCH #{batch_id} LIVE PREDICTIONS ({len(predicted_df)} txns) ═══")
            print(predicted_df[display_cols].head(15).to_string(index=False))
            if len(predicted_df) > 15:
                print(f"... and {len(predicted_df) - 15} more.")
            print("═" * 55 + "\n")

        # ── Persist fraud alerts to MongoDB ───────────────────────────────
        if not fraud_df.empty:
            alerts = []
            for _, row in fraud_df.iterrows():
                score    = float(row.get("anomaly_score", 0))
                severity = "HIGH" if score < -0.8 else ("MEDIUM" if score < -0.4 else "LOW")
                alert_doc = {
                    "transaction_id": str(row.get("transaction_id", "unknown")),
                    "event_time":     str(row.get("timestamp", "")),
                    "detection_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "type":           str(row.get("type", "")),
                    "amount":         float(row["amount"]),
                    "oldbalanceOrg":  float(row.get("oldbalanceOrg", 0)),
                    "newbalanceOrig": float(row.get("newbalanceOrig", 0)),
                    "oldbalanceDest": float(row.get("oldbalanceDest", 0)),
                    "newbalanceDest": float(row.get("newbalanceDest", 0)),
                    "prediction":     int(row["prediction"]),
                    "anomaly_score":  score,
                    "severity":       severity,
                }
                alerts.append(alert_doc)

            if db_client is not None:
                try:
                    inserted = db_client.insert_alerts_batch(alerts)
                    health.kpis.total_mongo_writes += inserted
                    health.mark_healthy("mongodb", f"Wrote {inserted} alerts")
                    logger.info("Batch #%d: Persisted %d/%d fraud alerts to MongoDB.",
                                batch_id, inserted, len(alerts))
                except Exception as e:
                    logger.error("Batch #%d: MongoDB write FAILED: %s", batch_id, e, exc_info=True)
                    health.kpis.total_errors += 1
                    health.mark_unhealthy("mongodb", f"Write error: {e}")
            else:
                logger.warning("Batch #%d: MongoDB unavailable — %d fraud alerts NOT persisted.",
                               batch_id, len(alerts))

    except Exception as e:
        logger.error("Batch #%d: UNEXPECTED error: %s", batch_id, e, exc_info=True)
        health.kpis.total_errors += 1

    finally:
        elapsed = (time.perf_counter() - t_start) * 1000
        logger.info("──── Micro-batch #%d END (%.1f ms) ────", batch_id, elapsed)


def get_spark_session(app_name: str = "FraudDetectionPipeline") -> SparkSession:
    """Create and return a configured SparkSession."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "10")
        .getOrCreate()
    )


def main() -> None:
    logger.info("=" * 70)
    logger.info("PRODUCTION PIPELINE — Real-Time Fraud Detection")
    logger.info("=" * 70)

    # ── 1. SparkSession ────────────────────────────────────────────────────
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    health.mark_healthy("spark", "Session created")

    listener = FraudStreamingListener()
    spark.streams.addListener(listener)

    # ── 2. Read from Kafka ─────────────────────────────────────────────────
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", KAFKA_BOOTSTRAP_SERVERS)
    topic             = os.getenv("KAFKA_TRANSACTIONS_TOPIC", TRANSACTIONS_TOPIC)

    logger.info("Connecting to Kafka: %s | Topic: %s", bootstrap_servers, topic)
    try:
        raw_df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )
        health.mark_healthy("kafka", f"Subscribed to {topic}")
    except Exception as e:
        logger.critical("FATAL — Cannot read Kafka stream: %s", e, exc_info=True)
        health.mark_unhealthy("kafka", str(e))
        spark.stop()
        return

    # ── 3. Parse JSON ──────────────────────────────────────────────────────
    parsed_df = (
        raw_df
        .selectExpr("CAST(value AS STRING) AS json_payload")
        .select(from_json(col("json_payload"), TRANSACTION_SCHEMA).alias("data"))
        .select("data.*")
    )

    # ── 4. Filter invalid records ──────────────────────────────────────────
    valid_types = ["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]
    clean_df = parsed_df.filter(
        col("amount").isNotNull() & (col("amount") >= 0) &
        col("oldbalanceOrg").isNotNull() &
        col("newbalanceOrig").isNotNull() &
        col("oldbalanceDest").isNotNull() &
        col("newbalanceDest").isNotNull() &
        col("type").isin(valid_types)
    )

    # ── 5. Start streaming ─────────────────────────────────────────────────
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

    query = (
        clean_df.writeStream
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", str(CHECKPOINT_DIR))
        .trigger(processingTime="5 seconds")
        .start()
    )

    logger.info("✅ Streaming query started — listening on topic: %s", topic)
    print_query_status(query, label="startup")
    health.log_status()

    # ── 6. Await with heartbeat ────────────────────────────────────────────
    try:
        while query.isActive:
            time.sleep(60)
            health.log_status()
            print_query_status(query, label="heartbeat")
    except KeyboardInterrupt:
        logger.info("⚠️  Shutdown signal — stopping streaming query...")
        query.stop()
    except Exception as exc:
        logger.error("Unexpected error in main loop: %s", exc, exc_info=True)
        query.stop()
    finally:
        if db_client:
            db_client.close()
        spark.streams.removeListener(listener)
        health.log_status()
        logger.info("PIPELINE SHUTDOWN COMPLETE.")


if __name__ == "__main__":
    main()
