"""Stream PaySim transactions into Kafka as JSON messages."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.kafka_config import (  # noqa: E402
    DEFAULT_DATASET_PATH,
    DEFAULT_PRODUCER_DELAY_SECONDS,
    KAFKA_BOOTSTRAP_SERVERS,
    TRANSACTIONS_TOPIC,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish PaySim transactions to the Kafka transactions topic.",
    )
    parser.add_argument(
        "--dataset",
        default=DEFAULT_DATASET_PATH,
        help="Path to the PaySim CSV file, relative to the project root if not absolute.",
    )
    parser.add_argument(
        "--topic",
        default=TRANSACTIONS_TOPIC,
        help="Kafka topic that receives transaction events.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=KAFKA_BOOTSTRAP_SERVERS,
        help="Kafka bootstrap server list for the producer.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_PRODUCER_DELAY_SECONDS,
        help="Seconds to wait between messages to simulate real-time traffic.",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Optional limit for test runs. Omit to stream the full dataset.",
    )
    parser.add_argument(
        "--create-topic",
        action="store_true",
        help="Create the Kafka topic before publishing if it does not exist.",
    )
    return parser.parse_args()


def resolve_dataset_path(dataset_path: str) -> Path:
    path = Path(dataset_path)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def normalize_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def row_to_transaction(row: pd.Series) -> dict[str, Any]:
    return {column: normalize_value(value) for column, value in row.to_dict().items()}


def create_topic_if_needed(bootstrap_servers: str, topic: str) -> None:
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id="fraud-topic-admin",
    )
    try:
        admin_client.create_topics(
            new_topics=[
                NewTopic(
                    name=topic,
                    num_partitions=1,
                    replication_factor=1,
                ),
            ],
            validate_only=False,
        )
        print(f"Created Kafka topic: {topic}")
    except TopicAlreadyExistsError:
        print(f"Kafka topic already exists: {topic}")
    finally:
        admin_client.close()


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        retries=5,
        linger_ms=10,
        bootstrap_timeout_ms=10_000,
        request_timeout_ms=10_000,
        max_block_ms=10_000,
    )


def stream_transactions(args: argparse.Namespace) -> int:
    dataset_path = resolve_dataset_path(args.dataset)
    if not dataset_path.exists():
        raise FileNotFoundError(f"PaySim dataset not found: {dataset_path}")

    if args.create_topic:
        create_topic_if_needed(args.bootstrap_servers, args.topic)

    df = pd.read_csv(dataset_path)
    if args.max_records is not None:
        df = df.head(args.max_records)

    producer = build_producer(args.bootstrap_servers)
    sent_count = 0

    try:
        for index, row in df.iterrows():
            transaction = row_to_transaction(row)
            message = json.dumps(transaction).encode("utf-8")
            metadata = producer.send(args.topic, value=message).get(timeout=10)
            sent_count += 1
            print(
                "Sent transaction "
                f"{index} to {metadata.topic}[{metadata.partition}] "
                f"offset {metadata.offset}: {transaction.get('type')} "
                f"{transaction.get('amount')}"
            )

            if args.delay > 0:
                time.sleep(args.delay)

        producer.flush()
    finally:
        producer.close()

    return sent_count


def main() -> None:
    args = parse_args()
    sent_count = stream_transactions(args)
    print(f"Finished streaming {sent_count} transactions to topic '{args.topic}'.")


if __name__ == "__main__":
    main()
