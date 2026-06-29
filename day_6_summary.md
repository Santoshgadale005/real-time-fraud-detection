# Day 6 Summary Report: Continuous Transaction Simulation & Advanced Kafka Producer

**Project**: Real-Time Financial Fraud Detection Pipeline  
**Date**: June 29, 2026  
**Author**: Santoshgadale005  

---

## 1. Objectives Completed

Today we upgraded the basic producer into a production-quality continuous transaction simulator:

* Created `config/producer_config.py` — all advanced producer settings centralised and env-var-overridable.
* Rewrote `producer/producer.py` with:
  * **Continuous loop** — `while True` cycles through the PaySim dataset indefinitely.
  * **UUID transaction IDs** — every event gets a unique `transaction_id` field.
  * **UTC timestamps** — every event gets a `timestamp` field for Spark event-time processing.
  * **Structured logging** — all `print()` replaced with `logging.info/error`.
  * **Per-message retry with exponential back-off** — up to 3 attempts, delay doubles on each failure.
  * **Batching** — `batch_size=16384` bytes, `linger_ms=5` for higher throughput.
  * **gzip compression** — reduces Kafka network bandwidth.
  * **Delivery confirmation** — `future.get(timeout=10)` ensures every message is acknowledged.
  * **Clean shutdown** — `KeyboardInterrupt` flushes and closes the producer before exiting.

---

## 2. Files Created & Updated

| File | Action | Description |
|------|--------|-------------|
| `config/producer_config.py` | **Created** | Central advanced producer settings (batching, compression, delay, continuous mode, retries) |
| `producer/producer.py` | **Rewritten** | Full Day 6 continuous simulator with UUIDs, timestamps, logging, retry, batching, compression |
| `README.md` | **Updated** | New continuous producer section, enriched transaction schema, producer config reference table, Day 6 progress |
| `day_6_summary.md` | **Created** | This report |

---

## 3. Producer Configuration Details

Settings are stored in `config/producer_config.py` and can be overridden via environment variables:

| Setting | Env Var | Default | Description |
|---------|---------|---------|-------------|
| Bootstrap servers | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| Topic | `KAFKA_TRANSACTIONS_TOPIC` | `transactions` | Target topic |
| Stream delay | `PRODUCER_DELAY_SECONDS` | `0.2` | Seconds between messages |
| Continuous | `PRODUCER_CONTINUOUS` | `true` | Loop forever |
| Batch size | `PRODUCER_BATCH_SIZE` | `16384` | Bytes per batch (16 KB) |
| Linger | `PRODUCER_LINGER_MS` | `5` | ms to wait for batch to fill |
| Compression | `PRODUCER_COMPRESSION_TYPE` | `gzip` | Network compression codec |
| Retries | `PRODUCER_RETRIES` | `5` | Kafka auto-retry count |

---

## 4. Transaction Enrichment (New Fields)

Every transaction published to Kafka now includes two new fields injected by the producer:

```json
{
    "step": 1,
    "type": "TRANSFER",
    "amount": 9839.64,
    "nameOrig": "C1231006815",
    "nameDest": "M1979787155",
    "isFraud": 0,
    "transaction_id": "f3a2b1c4-d5e6-7890-abcd-ef1234567890",
    "timestamp": "2026-06-29T08:00:00.123456+00:00"
}
```

| Field | Type | Purpose |
|-------|------|---------|
| `transaction_id` | UUID string | Unique event identifier — no duplicate processing |
| `timestamp` | UTC ISO-8601 | Pipeline ingestion time — used by Spark for event-time windows |

---

## 5. Retry Logic

The producer uses a three-attempt retry with exponential back-off:

```text
Attempt 1  →  fails  →  wait 1.0 s
Attempt 2  →  fails  →  wait 2.0 s
Attempt 3  →  fails  →  log error, drop message, continue
```

This handles transient network interruptions without crashing the simulator.

---

## 6. Batching & Compression

| Feature | Setting | Benefit |
|---------|---------|---------|
| Batch size | 16 384 bytes | Groups multiple messages into one network call |
| Linger | 5 ms | Waits briefly to fill the batch before sending |
| Compression | gzip | Reduces Kafka network bandwidth by ~60–70 % for JSON |

---

## 7. How to Run Day 6

### Step 1 — Verify Kafka infrastructure

```bash
docker ps
```

Expected: `zookeeper`, `kafka`, `mongodb`, `kafka-ui` all running.

### Step 2 — Quick smoke test (10 records, exits)

```bash
python3 producer/producer.py --no-continuous --create-topic --max-records 10 --delay 0.5
```

### Step 3 — Continuous simulation (runs until Ctrl-C)

```bash
python3 producer/producer.py --continuous
```

### Step 4 — Validate in a second terminal

```bash
python3 consumer/consumer.py
```

### Expected producer log output

```text
2026-06-29 08:00:00 [INFO] fraud-producer — Kafka producer connected to localhost:9092
2026-06-29 08:00:00 [INFO] fraud-producer —   Topic       : transactions
2026-06-29 08:00:00 [INFO] fraud-producer —   Compression : gzip
2026-06-29 08:00:00 [INFO] fraud-producer —   Delay       : 0.200 s/msg
2026-06-29 08:00:00 [INFO] fraud-producer —   Continuous  : True
2026-06-29 08:00:00 [INFO] fraud-producer — --- Starting dataset cycle 1 ---
2026-06-29 08:00:00 [INFO] fraud-producer — Sent transaction f3a2b1c4-...  [TRANSFER]  amount=9839.64  fraud=0  → transactions[0] offset 5
2026-06-29 08:00:00 [INFO] fraud-producer — Sent transaction a1b2c3d4-...  [PAYMENT]   amount=1864.28  fraud=0  → transactions[0] offset 6
...
```

---

## 8. End-to-End Pipeline (Day 6)

```text
PaySim Dataset
    ↓
Continuous Producer  (producer/producer.py)
    ↓  UUIDs + Timestamps + gzip + Retry + Batching
Kafka Topic: transactions
    ↓
Python Consumer  (consumer/consumer.py)
```

---

## 9. Connection to Tomorrow's Work

Day 6 created a realistic, never-ending financial transaction stream.

Day 7 will validate the entire Week 1 streaming infrastructure:

```text
End-to-end pipeline validation
Kafka throughput testing
Consumer lag and offsets
Pipeline health checks
Project architecture review
Week 1 integration testing
```

This sets the foundation for Week 2 (model training) and Week 3 (Spark Structured Streaming).
