"""Advanced Kafka Producer configuration for the local fraud detection pipeline.

All values can be overridden at runtime via environment variables or CLI flags.
"""

from os import getenv

from config.kafka_config import (
    DEFAULT_DATASET_PATH,
    DEFAULT_PRODUCER_DELAY_SECONDS,
    KAFKA_BOOTSTRAP_SERVERS,
    TRANSACTIONS_TOPIC,
)

# ---------------------------------------------------------------------------
# Bootstrap server & topic (re-exported for producer convenience)
# ---------------------------------------------------------------------------
PRODUCER_BOOTSTRAP_SERVERS = KAFKA_BOOTSTRAP_SERVERS
PRODUCER_TOPIC = TRANSACTIONS_TOPIC

# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------
PRODUCER_DATASET_PATH = DEFAULT_DATASET_PATH

# ---------------------------------------------------------------------------
# Throughput / timing
# ---------------------------------------------------------------------------
# Seconds to sleep between messages (0.2 ≈ 5 msg/s — realistic card traffic)
PRODUCER_STREAM_DELAY = float(getenv("PRODUCER_DELAY_SECONDS", str(DEFAULT_PRODUCER_DELAY_SECONDS)))

# ---------------------------------------------------------------------------
# Continuous mode
# If True the producer loops through the dataset indefinitely.
# If False it streams the dataset once and exits.
# ---------------------------------------------------------------------------
PRODUCER_CONTINUOUS = getenv("PRODUCER_CONTINUOUS", "true").lower() == "true"

# ---------------------------------------------------------------------------
# Batching
# batch_size  : max bytes accumulated before a batch is sent (16 KB default)
# linger_ms   : max milliseconds to wait for more records to fill the batch
# ---------------------------------------------------------------------------
PRODUCER_BATCH_SIZE = int(getenv("PRODUCER_BATCH_SIZE", "16384"))   # 16 KB
PRODUCER_LINGER_MS  = int(getenv("PRODUCER_LINGER_MS",  "5"))

# ---------------------------------------------------------------------------
# Compression
# Reduces network bandwidth. Options: None | "gzip" | "snappy" | "lz4" | "zstd"
# ---------------------------------------------------------------------------
PRODUCER_COMPRESSION_TYPE = getenv("PRODUCER_COMPRESSION_TYPE", "gzip")

# ---------------------------------------------------------------------------
# Reliability
# acks     : "all" waits for leader + all in-sync replicas to confirm
# retries  : number of automatic retry attempts on transient failures
# ---------------------------------------------------------------------------
PRODUCER_ACKS    = getenv("PRODUCER_ACKS", "all")
PRODUCER_RETRIES = int(getenv("PRODUCER_RETRIES", "5"))

# ---------------------------------------------------------------------------
# Timeouts (milliseconds)
# ---------------------------------------------------------------------------
PRODUCER_REQUEST_TIMEOUT_MS  = int(getenv("PRODUCER_REQUEST_TIMEOUT_MS",  "10000"))
PRODUCER_MAX_BLOCK_MS        = int(getenv("PRODUCER_MAX_BLOCK_MS",        "10000"))
