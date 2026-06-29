"""Continuously read PaySim transactions from Kafka and print them to stdout.

Usage
-----
# Consume all available messages from the beginning:
python3 consumer/consumer.py

# Consume only 20 messages then exit:
python3 consumer/consumer.py --max-records 20

# Override Kafka broker and topic at runtime:
python3 consumer/consumer.py --bootstrap-servers localhost:9092 --topic transactions

# Use a different consumer group:
python3 consumer/consumer.py --group-id my-test-group

Press Ctrl-C at any time to stop cleanly.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

from kafka import KafkaConsumer

# ---------------------------------------------------------------------------
# Resolve the project root so that the package can be imported regardless of
# the working directory from which the script is executed.
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.consumer_config import (  # noqa: E402
    CONSUMER_AUTO_OFFSET_RESET,
    CONSUMER_BOOTSTRAP_SERVERS,
    CONSUMER_GROUP_ID,
    CONSUMER_MAX_RECORDS,
    CONSUMER_TOPIC,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("fraud-consumer")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Kafka consumer — reads live PaySim transactions from Kafka.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=CONSUMER_BOOTSTRAP_SERVERS,
        help="Kafka broker address (default: %(default)s).",
    )
    parser.add_argument(
        "--topic",
        default=CONSUMER_TOPIC,
        help="Kafka topic to subscribe to (default: %(default)s).",
    )
    parser.add_argument(
        "--group-id",
        default=CONSUMER_GROUP_ID,
        help="Consumer group ID (default: %(default)s).",
    )
    parser.add_argument(
        "--offset-reset",
        default=CONSUMER_AUTO_OFFSET_RESET,
        choices=["earliest", "latest"],
        help=(
            "Where to start reading when no committed offset exists. "
            "'earliest' reads all stored messages; 'latest' reads only new ones. "
            "(default: %(default)s)"
        ),
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=CONSUMER_MAX_RECORDS,
        help="Stop after consuming this many messages. Omit for unlimited (default).",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print each transaction as indented JSON.",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Consumer helpers
# ---------------------------------------------------------------------------

def build_consumer(
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    auto_offset_reset: str,
) -> KafkaConsumer:
    """Create and return a configured KafkaConsumer instance."""
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
        consumer_timeout_ms=30_000,   # stop iterating after 30 s of silence
    )


def format_transaction(transaction: dict[str, Any], pretty: bool = False) -> str:
    """Return a human-readable string for one transaction."""
    if pretty:
        return json.dumps(transaction, indent=4)

    tx_type   = transaction.get("type",     "UNKNOWN")
    amount    = transaction.get("amount",   0)
    is_fraud  = transaction.get("isFraud",  "?")
    sender    = transaction.get("nameOrig", "?")
    receiver  = transaction.get("nameDest", "?")
    return (
        f"[{tx_type:>10}] "
        f"amount={amount:>12.2f}  "
        f"fraud={is_fraud}  "
        f"{sender} → {receiver}"
    )


# ---------------------------------------------------------------------------
# Main consume loop
# ---------------------------------------------------------------------------

def consume(args: argparse.Namespace) -> int:
    """Connect to Kafka, read messages, and return the total consumed."""
    logger.info("Connecting to Kafka broker at %s …", args.bootstrap_servers)
    logger.info("  Topic        : %s", args.topic)
    logger.info("  Group ID     : %s", args.group_id)
    logger.info("  Offset reset : %s", args.offset_reset)
    logger.info(
        "  Max records  : %s",
        args.max_records if args.max_records else "unlimited",
    )
    logger.info("Press Ctrl-C to stop.\n")

    consumer = build_consumer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id,
        auto_offset_reset=args.offset_reset,
    )

    consumed = 0

    try:
        for message in consumer:
            transaction: dict[str, Any] = message.value

            # --- Offset info ---------------------------------------------------
            offset_info = (
                f"partition={message.partition}  "
                f"offset={message.offset}"
            )

            logger.info(
                "Received Transaction #%d  [%s]",
                consumed + 1,
                offset_info,
            )

            # --- Display transaction fields ------------------------------------
            print(format_transaction(transaction, pretty=args.pretty))

            consumed += 1

            # --- Optional early exit -------------------------------------------
            if args.max_records and consumed >= args.max_records:
                logger.info(
                    "Reached max-records limit (%d). Stopping.", args.max_records
                )
                break

    except KeyboardInterrupt:
        logger.info("Interrupted by user (Ctrl-C). Shutting down …")
    finally:
        consumer.close()
        logger.info("Consumer closed. Total messages consumed: %d", consumed)

    return consumed


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    total = consume(args)
    print(f"\n✅  End-to-end streaming validated — consumed {total} transactions.")


if __name__ == "__main__":
    main()
