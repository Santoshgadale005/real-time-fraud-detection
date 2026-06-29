"""Kafka Consumer configuration for the local fraud detection pipeline."""

from os import getenv

from config.kafka_config import KAFKA_BOOTSTRAP_SERVERS, TRANSACTIONS_TOPIC

# ---------------------------------------------------------------------------
# Consumer identity
# ---------------------------------------------------------------------------
CONSUMER_GROUP_ID = getenv("KAFKA_CONSUMER_GROUP_ID", "fraud-detection-group")

# ---------------------------------------------------------------------------
# Bootstrap servers (re-exported for consumer convenience)
# ---------------------------------------------------------------------------
CONSUMER_BOOTSTRAP_SERVERS = KAFKA_BOOTSTRAP_SERVERS

# ---------------------------------------------------------------------------
# Topic the consumer subscribes to
# ---------------------------------------------------------------------------
CONSUMER_TOPIC = TRANSACTIONS_TOPIC

# ---------------------------------------------------------------------------
# Offset reset policy
# "earliest" → read all messages from the beginning if no committed offset exists.
# "latest"   → read only new messages arriving after the consumer starts.
# ---------------------------------------------------------------------------
CONSUMER_AUTO_OFFSET_RESET = getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")

# ---------------------------------------------------------------------------
# Optional: number of messages to consume before stopping (None = unlimited)
# ---------------------------------------------------------------------------
CONSUMER_MAX_RECORDS = (
    int(getenv("CONSUMER_MAX_RECORDS"))
    if getenv("CONSUMER_MAX_RECORDS")
    else None
)
