# Real-Time Financial Fraud Detection Pipeline

An end-to-end streaming and MLOps project that simulates real-time financial fraud detection using Kafka, Spark Structured Streaming, MongoDB, Prometheus, and Grafana.

## Problem Statement

Detect suspicious financial transactions as they arrive, score them with a fraud detection model, store fraud alerts, and monitor the health of the streaming system.

## Architecture

```text
PaySim Dataset
    ↓
Python Producer
    ↓
Kafka Topic: transactions
    ↓
Python Consumer  ← (Day 5)
    ↓  (replaced by Spark Structured Streaming in Week 3)
Spark Structured Streaming
    ↓
Fraud Detection Model
    ↓
MongoDB Fraud Alerts
    ↓
Grafana Dashboard
```

## Tech Stack

- **Python**: producer, consumer, feature engineering, model development
- **Apache Kafka**: event streaming backbone
- **Apache Spark Structured Streaming**: real-time transaction processing
- **MongoDB**: fraud alert storage
- **Scikit-Learn**: Isolation Forest fraud detection model
- **Docker Compose**: local infrastructure orchestration
- **Prometheus & Grafana**: metrics and monitoring
- **Kafka UI**: local Kafka topic/message inspection

## Directory Structure

```text
real-time-fraud-detection/
├── config/             # Application configuration variables
│   ├── kafka_config.py     # Shared Kafka settings (broker, topic)
│   ├── producer_config.py  # Continuous producer settings (batching, compression, delay)
│   └── consumer_config.py  # Consumer-specific settings (group, offset)
├── consumer/           # Kafka consumer utilities
│   └── consumer.py         # Live transaction reader with JSON deserialization
├── dashboards/         # Grafana dashboard exports
├── data/               # PaySim raw and engineered datasets
├── docker/             # Custom Docker assets
├── docs/               # Architecture, workflow, and data documentation
├── models/             # Feature engineering and model artifacts
├── monitoring/         # Prometheus and monitoring configuration
├── notebooks/          # EDA and prototyping notebooks
├── producer/           # Kafka transaction producer
│   └── producer.py         # Continuous simulator with UUID IDs, timestamps, retries
├── reports/            # Detailed project reports
├── spark/              # Spark Structured Streaming jobs
├── docker-compose.yml  # Local service orchestration
├── requirements.txt    # Python dependency manifest
└── README.md
```

## Local Infrastructure

Start all services:

```bash
docker compose up -d
```

Verify running containers:

```bash
docker ps
```

Expected containers:

```text
zookeeper
kafka
mongodb
kafka-ui
```

Kafka UI is available at:

```text
http://localhost:8080
```

## Kafka Configuration

The project uses one transaction topic during the streaming foundation phase:

```text
Topic: transactions
Partitions: 1
Replication factor: 1
Host bootstrap server: localhost:9092
Container bootstrap server: kafka:29092
```

Shared settings are loaded from:

```text
config/kafka_config.py     ← broker address, topic name
config/producer_config.py  ← batching, compression, continuous mode, retries
config/consumer_config.py  ← consumer group ID, offset reset policy
```

## Create the Kafka Topic Manually

```bash
docker exec kafka kafka-topics \
  --create \
  --topic transactions \
  --bootstrap-server localhost:9092
```

Verify topics:

```bash
docker exec kafka kafka-topics \
  --list \
  --bootstrap-server localhost:9092
```

## Run the Continuous Transaction Producer (Day 6)

Install dependencies if needed:

```bash
pip install -r requirements.txt
```

### Quick smoke test (10 records, exits after):

```bash
python3 producer/producer.py --no-continuous --create-topic --max-records 10 --delay 0.5
```

### Continuous mode (loops forever — simulates a live payment gateway):

```bash
python3 producer/producer.py --continuous
```

### Continuous mode with faster throughput:

```bash
python3 producer/producer.py --continuous --delay 0.1
```

Press **Ctrl-C** at any time to flush remaining messages and shut down cleanly.

### All producer options:

```text
--continuous          Loop through the dataset indefinitely (default: true)
--no-continuous       Stream the dataset once then exit
--create-topic        Create the Kafka topic before publishing
--max-records N       Stop after N messages regardless of mode
--delay SECONDS       Seconds between messages (default: 0.2)
--dataset PATH        Use a different CSV file
--bootstrap-servers   Override the Kafka broker address
--compression         gzip | snappy | lz4 | zstd | none (default: gzip)
```

### What each transaction now includes (Day 6 enrichment):

```json
{
    "step": 1,
    "type": "TRANSFER",
    "amount": 9839.64,
    "nameOrig": "C1231006815",
    "nameDest": "M1979787155",
    "isFraud": 0,
    "transaction_id": "f3a2b1c4-...",
    "timestamp": "2026-06-29T08:00:00.000000+00:00"
}
```

| New field | Purpose |
|-----------|---------|
| `transaction_id` | UUID — unique identifier for every event |
| `timestamp` | UTC ISO-8601 — records when the event entered the pipeline |

## Run the Transaction Consumer

Consume all messages from the beginning (reads everything already in Kafka):

```bash
python3 consumer/consumer.py
```

Consume only 20 messages then exit:

```bash
python3 consumer/consumer.py --max-records 20
```

Pretty-print each transaction as indented JSON:

```bash
python3 consumer/consumer.py --pretty
```

Useful consumer options:

```text
--bootstrap-servers  Override the Kafka broker address
--topic              Override the topic name
--group-id           Override the consumer group ID
--offset-reset       'earliest' (default) or 'latest'
--max-records N      Stop after N messages
--pretty             Pretty-print each transaction as indented JSON
```

Press **Ctrl-C** at any time to shut down cleanly.

## End-to-End Streaming Validation (Day 5)

### Terminal 1 — Start the Consumer

```bash
python3 consumer/consumer.py
```

Consumer will wait, displaying:

```text
Connecting to Kafka broker at localhost:9092 …
  Topic        : transactions
  Group ID     : fraud-detection-group
  Offset reset : earliest
Press Ctrl-C to stop.
```

### Terminal 2 — Run the Producer

```bash
python3 producer/producer.py --max-records 10 --delay 0.5
```

### Expected Consumer Output

```text
2026-06-25 12:00:00 [INFO] fraud-consumer — Received Transaction #1  [partition=0  offset=0]
[  TRANSFER]  amount=     9839.64  fraud=0  C1231006815 → M1979787155
2026-06-25 12:00:01 [INFO] fraud-consumer — Received Transaction #2  [partition=0  offset=1]
[   PAYMENT]  amount=     1864.28  fraud=0  C1666544295 → M2044282225
...
✅  End-to-end streaming validated — consumed 10 transactions.
```

## Verify Messages in Kafka UI

Open:

```text
http://localhost:8080
```

Navigate to:

```text
Topics → transactions → Messages
```

Expected result:

```text
PaySim transaction records appear as JSON messages.
```

## Consumer Groups

The consumer runs under the group ID `fraud-detection-group` (configurable via `--group-id`).

- Kafka tracks the last-read offset per group, preventing duplicate processing on restart.
- Multiple consumers in the same group share the partition load automatically.
- In Week 3, Spark Structured Streaming workers will behave as a consumer group.

## Offset Behaviour

| Flag | Behaviour |
|------|-----------|
| `--offset-reset earliest` | Reads all messages already stored in Kafka (default) |
| `--offset-reset latest`   | Reads only new messages arriving after the consumer starts |

## Producer Configuration Reference (Day 6)

All producer settings live in `config/producer_config.py` and can be overridden via environment variables:

| Setting | Env Var | Default | Description |
|---------|---------|---------|-------------|
| Bootstrap servers | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| Topic | `KAFKA_TRANSACTIONS_TOPIC` | `transactions` | Target topic |
| Stream delay | `PRODUCER_DELAY_SECONDS` | `0.2` | Seconds between messages |
| Continuous | `PRODUCER_CONTINUOUS` | `true` | Loop forever |
| Batch size | `PRODUCER_BATCH_SIZE` | `16384` | Bytes per batch |
| Linger | `PRODUCER_LINGER_MS` | `5` | ms to wait before sending batch |
| Compression | `PRODUCER_COMPRESSION_TYPE` | `gzip` | Network compression |
| Retries | `PRODUCER_RETRIES` | `5` | Auto-retry count |

## Current Progress

- **Day 1**: Environment setup and Docker infrastructure
- **Day 2**: PaySim dataset generation and fraud EDA
- **Day 3**: Fraud pattern investigation and feature engineering
- **Day 4**: Kafka fundamentals and transaction producer
- **Day 5**: Kafka consumer and end-to-end streaming validation ✅
- **Day 6**: Continuous producer, UUIDs, timestamps, batching, compression, retries ✅

## Next Step

Day 7 concludes Week 1 with **Streaming Pipeline Validation & Infrastructure Testing**:

```text
End-to-end pipeline validation
Kafka throughput testing
Consumer lag and offsets
Pipeline health checks
Project architecture review
Week 1 integration testing
```
