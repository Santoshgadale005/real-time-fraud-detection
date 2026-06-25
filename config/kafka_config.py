"""Kafka configuration for the local fraud detection pipeline."""

from os import getenv


KAFKA_BOOTSTRAP_SERVERS = getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_INTERNAL_BOOTSTRAP_SERVERS = getenv(
    "KAFKA_INTERNAL_BOOTSTRAP_SERVERS",
    "kafka:29092",
)
TRANSACTIONS_TOPIC = getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")

DEFAULT_PRODUCER_DELAY_SECONDS = float(getenv("PRODUCER_DELAY_SECONDS", "0.1"))
DEFAULT_DATASET_PATH = getenv("PAYSIM_DATASET_PATH", "data/paysim.csv")
