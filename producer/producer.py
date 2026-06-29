"""Continuous PaySim transaction simulator — streams transactions into Kafka forever.

Day 6 upgrades over the Day 4/5 producer:
  * Continuous loop  — cycles through the dataset indefinitely (--continuous flag)
  * Unique IDs       — every transaction gets a UUID transaction_id field
  * Timestamps       — every transaction gets a UTC ISO-8601 timestamp field
  * Structured logging — replaces all print() calls with logging.info/error
  * Retry logic      — per-message send retries with exponential back-off
  * Batching         — KafkaProducer batch_size + linger_ms for throughput
  * Compression      — gzip by default to reduce network bandwidth
  * Delivery confirm — future.get(timeout) ensures every message was received

Usage
-----
# Continuous mode (loops forever, Ctrl-C to stop):
python3 producer/producer.py --continuous

# One-shot mode (streams dataset once then exits):
python3 producer/producer.py

# Quick smoke test (10 records, 0.5 s delay, continuous off):
python3 producer/producer.py --max-records 10 --delay 0.5

# Create topic automatically:
python3 producer/producer.py --create-topic --continuous
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

# ---------------------------------------------------------------------------
# Project root resolution (so config package is importable from any cwd)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.producer_config import (  # noqa: E402
    PRODUCER_ACKS,
    PRODUCER_BATCH_SIZE,
    PRODUCER_BOOTSTRAP_SERVERS,
    PRODUCER_COMPRESSION_TYPE,
    PRODUCER_CONTINUOUS,
    PRODUCER_DATASET_PATH,
    PRODUCER_LINGER_MS,
    PRODUCER_MAX_BLOCK_MS,
    PRODUCER_REQUEST_TIMEOUT_MS,
    PRODUCER_RETRIES,
    PRODUCER_STREAM_DELAY,
    PRODUCER_TOPIC,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("fraud-producer")

# ---------------------------------------------------------------------------
# Retry settings for per-message send failures
# ---------------------------------------------------------------------------
_SEND_MAX_ATTEMPTS = 3
_SEND_RETRY_BASE_DELAY = 1.0  # seconds; doubles on each retry


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Continuous PaySim transaction simulator. "
            "Publishes transactions to a Kafka topic as JSON events."
        ),
    )
    parser.add_argument(
        "--dataset",
        default=PRODUCER_DATASET_PATH,
        help="Path to the PaySim CSV file (default: %(default)s).",
    )
    parser.add_argument(
        "--topic",
        default=PRODUCER_TOPIC,
        help="Kafka topic to publish to (default: %(default)s).",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=PRODUCER_BOOTSTRAP_SERVERS,
        help="Kafka broker address (default: %(default)s).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=PRODUCER_STREAM_DELAY,
        help="Seconds between messages to throttle throughput (default: %(default)s).",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Stop after sending this many records. Omit for unlimited (default).",
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        default=PRODUCER_CONTINUOUS,
        help=(
            "Loop through the dataset indefinitely. "
            "Press Ctrl-C to stop cleanly (default: %(default)s)."
        ),
    )
    parser.add_argument(
        "--no-continuous",
        dest="continuous",
        action="store_false",
        help="Stream the dataset once then exit.",
    )
    parser.add_argument(
        "--create-topic",
        action="store_true",
        help="Create the Kafka topic before publishing if it does not exist.",
    )
    parser.add_argument(
        "--compression",
        default=PRODUCER_COMPRESSION_TYPE,
        choices=["gzip", "snappy", "lz4", "zstd", "none"],
        help="Compression codec for Kafka messages (default: %(default)s).",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def resolve_dataset_path(dataset_path: str) -> Path:
    path = Path(dataset_path)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def normalize_value(value: Any) -> Any:
    """Convert pandas/numpy types to JSON-safe Python primitives."""
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        return value.item()
    return value


def row_to_transaction(row: pd.Series) -> dict[str, Any]:
    """Convert a DataFrame row to a JSON-safe transaction dict."""
    return {col: normalize_value(val) for col, val in row.to_dict().items()}


def enrich_transaction(transaction: dict[str, Any]) -> dict[str, Any]:
    """Add a unique transaction_id and a UTC timestamp to the transaction.

    These fields are added *after* the CSV columns so they never overwrite
    existing data and are always present regardless of the dataset schema.
    """
    transaction["transaction_id"] = str(uuid.uuid4())
    transaction["timestamp"] = datetime.now(timezone.utc).isoformat()
    return transaction


# ---------------------------------------------------------------------------
# Topic management
# ---------------------------------------------------------------------------

def create_topic_if_needed(bootstrap_servers: str, topic: str) -> None:
    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id="fraud-topic-admin",
    )
    try:
        admin.create_topics(
            new_topics=[
                NewTopic(name=topic, num_partitions=1, replication_factor=1)
            ],
            validate_only=False,
        )
        logger.info("Created Kafka topic: %s", topic)
    except TopicAlreadyExistsError:
        logger.info("Kafka topic already exists: %s", topic)
    finally:
        admin.close()


# ---------------------------------------------------------------------------
# Producer factory
# ---------------------------------------------------------------------------

def build_producer(bootstrap_servers: str, compression: str) -> KafkaProducer:
    """Return a KafkaProducer configured for batching, compression and reliability."""
    compression_type = None if compression == "none" else compression
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks=PRODUCER_ACKS,
        retries=PRODUCER_RETRIES,
        batch_size=PRODUCER_BATCH_SIZE,
        linger_ms=PRODUCER_LINGER_MS,
        compression_type=compression_type,
        request_timeout_ms=PRODUCER_REQUEST_TIMEOUT_MS,
        max_block_ms=PRODUCER_MAX_BLOCK_MS,
    )


# ---------------------------------------------------------------------------
# Per-message send with retry + exponential back-off
# ---------------------------------------------------------------------------

def send_with_retry(
    producer: KafkaProducer,
    topic: str,
    message: bytes,
    transaction_id: str,
) -> Any:
    """Send one message to Kafka with up to _SEND_MAX_ATTEMPTS retries.

    Returns the RecordMetadata on success.
    Raises the last KafkaError if all attempts fail.
    """
    last_error: Exception | None = None
    delay = _SEND_RETRY_BASE_DELAY

    for attempt in range(1, _SEND_MAX_ATTEMPTS + 1):
        try:
            future = producer.send(topic, value=message)
            metadata = future.get(timeout=10)
            return metadata
        except KafkaError as exc:
            last_error = exc
            logger.error(
                "Send failed for transaction %s (attempt %d/%d): %s",
                transaction_id,
                attempt,
                _SEND_MAX_ATTEMPTS,
                exc,
            )
            if attempt < _SEND_MAX_ATTEMPTS:
                logger.info("Retrying in %.1f s …", delay)
                time.sleep(delay)
                delay *= 2  # exponential back-off
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            logger.error(
                "Unexpected error sending transaction %s: %s",
                transaction_id,
                exc,
            )
            break

    raise last_error  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Core streaming loop
# ---------------------------------------------------------------------------

def stream_transactions(args: argparse.Namespace) -> int:
    """Stream transactions from the PaySim CSV into Kafka.

    When --continuous is set the loop repeats indefinitely.
    Returns the total number of messages successfully sent.
    """
    dataset_path = resolve_dataset_path(args.dataset)
    if not dataset_path.exists():
        raise FileNotFoundError(f"PaySim dataset not found: {dataset_path}")

    if args.create_topic:
        create_topic_if_needed(args.bootstrap_servers, args.topic)

    logger.info("Loading dataset from %s …", dataset_path)
    df = pd.read_csv(dataset_path)
    logger.info("Dataset loaded — %d rows available.", len(df))

    producer = build_producer(args.bootstrap_servers, args.compression)
    logger.info("Kafka producer connected to %s", args.bootstrap_servers)
    logger.info("  Topic       : %s", args.topic)
    logger.info("  Compression : %s", args.compression)
    logger.info("  Batch size  : %d bytes", PRODUCER_BATCH_SIZE)
    logger.info("  Linger      : %d ms", PRODUCER_LINGER_MS)
    logger.info("  Delay       : %.3f s/msg", args.delay)
    logger.info("  Continuous  : %s", args.continuous)
    logger.info("  Max records : %s", args.max_records or "unlimited")
    logger.info("Press Ctrl-C to stop cleanly.\n")

    total_sent = 0
    cycle = 0

    try:
        while True:
            cycle += 1
            if args.continuous:
                logger.info("--- Starting dataset cycle %d ---", cycle)

            rows = df.iterrows()

            for _index, row in rows:
                # Build and enrich the transaction
                transaction = row_to_transaction(row)
                transaction = enrich_transaction(transaction)

                tx_id = transaction["transaction_id"]
                tx_type = transaction.get("type", "UNKNOWN")
                tx_amount = transaction.get("amount", 0)
                tx_fraud = transaction.get("isFraud", "?")

                # Serialize to JSON bytes
                message = json.dumps(transaction).encode("utf-8")

                # Send with retry
                try:
                    metadata = send_with_retry(producer, args.topic, message, tx_id)
                    total_sent += 1
                    logger.info(
                        "Sent transaction %s  [%s]  amount=%.2f  fraud=%s  "
                        "→ %s[%d] offset %d",
                        tx_id,
                        tx_type,
                        tx_amount,
                        tx_fraud,
                        metadata.topic,
                        metadata.partition,
                        metadata.offset,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.error(
                        "Dropping transaction %s after all retries: %s", tx_id, exc
                    )

                # Throttle
                if args.delay > 0:
                    time.sleep(args.delay)

                # Honour --max-records
                if args.max_records and total_sent >= args.max_records:
                    logger.info(
                        "Reached max-records limit (%d). Stopping.", args.max_records
                    )
                    return total_sent

            # Single-pass mode exits after the first cycle
            if not args.continuous:
                break

    except KeyboardInterrupt:
        logger.info("Interrupted by user (Ctrl-C). Flushing and shutting down …")
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer closed. Total messages sent: %d", total_sent)

    return total_sent


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    total = stream_transactions(args)
    logger.info(
        "✅  Finished — %d transactions published to topic '%s'.",
        total,
        args.topic,
    )


if __name__ == "__main__":
    main()
