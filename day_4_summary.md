# Day 4 Summary Report: Apache Kafka Fundamentals & Transaction Producer

**Project**: Real-Time Financial Fraud Detection Pipeline  
**Date**: June 25, 2026  
**Author**: Santoshgadale005  

---

## 1. Objectives Completed
Today we moved from offline data preparation into real-time streaming foundations. We completed the following milestones:
* Reviewed Kafka architecture and the role of brokers, topics, producers, consumers, partitions, and offsets.
* Confirmed the local Docker architecture uses Kafka on `localhost:9092` for host applications and `kafka:29092` for containers.
* Created a dedicated Kafka configuration module to avoid hardcoding broker and topic names.
* Built a Python Kafka producer that reads PaySim transactions from CSV and publishes them as JSON messages.
* Added topic creation support so the producer can create the `transactions` topic when needed.
* Documented producer commands, Kafka UI verification, and the Day 4 streaming flow in the README.

---

## 2. Files Created & Updated
* **`config/kafka_config.py`**: Central Kafka settings for bootstrap servers, topic name, dataset path, and default producer delay.
* **`config/__init__.py`**: Converts the config directory into an importable Python package.
* **`producer/producer.py`**: Streams rows from `data/paysim.csv` into Kafka as UTF-8 encoded JSON messages.
* **`producer/__init__.py`**: Converts the producer directory into an importable Python package.
* **`README.md`**: Updated with Kafka architecture, topic details, setup commands, producer usage, and verification steps.
* **`day_4_summary.md`**: This summary report documenting the completed Day 4 deliverables.

---

## 3. Kafka Topic Details
The project now uses the following topic for transaction events:

```text
Topic: transactions
Partitions: 1
Replication factor: 1
Host bootstrap server: localhost:9092
Container bootstrap server: kafka:29092
```

One partition is enough for the current local development pipeline. Later, partitions can be increased when we test higher throughput or multiple consumers.

---

## 4. Producer Implementation Details
The producer performs the following steps:
1. Loads PaySim transaction rows from `data/paysim.csv`.
2. Converts each row into a Python dictionary.
3. Normalizes pandas/numpy values into JSON-safe Python values.
4. Serializes each transaction into JSON bytes.
5. Publishes the message to the `transactions` Kafka topic.
6. Sleeps between messages to simulate real-time transaction arrival.
7. Flushes and closes the producer cleanly before exiting.

The producer also supports useful development flags:
* `--create-topic`: Creates the topic before streaming.
* `--max-records`: Sends only a small number of records for testing.
* `--delay`: Controls the delay between messages.
* `--dataset`: Overrides the CSV input path.
* `--bootstrap-servers`: Overrides the Kafka broker address.

---

## 5. How to Run Day 4
Start infrastructure:

```bash
docker compose up -d
```

Create the topic and stream 10 test transactions:

```bash
python3 producer/producer.py --create-topic --max-records 10 --delay 0.1
```

Stream the full dataset:

```bash
python3 producer/producer.py
```

Verify messages in Kafka UI:

```text
http://localhost:8080
```

Navigate to:

```text
Topics -> transactions -> Messages
```

---

## 6. Verification Completed
The local Docker services were verified as running:
* `zookeeper`
* `kafka`
* `mongodb`
* `kafka-ui`

The `transactions` topic was created and listed successfully. A five-record producer smoke test was also completed:

```text
Sent transaction 0 to transactions[0] offset 0
Sent transaction 1 to transactions[0] offset 1
Sent transaction 2 to transactions[0] offset 2
Sent transaction 3 to transactions[0] offset 3
Sent transaction 4 to transactions[0] offset 4
```

Kafka offset verification returned:

```text
transactions:0:5
```

This confirms that five PaySim transactions were published to partition `0` of the `transactions` topic.

---

## 7. Connection to Tomorrow's Work
Day 4 created the real-time data source for the project:

```text
PaySim Dataset
    ↓
Python Producer
    ↓
Kafka Topic: transactions
```

Day 5 will build on this by creating a Kafka consumer and validating the full producer-to-Kafka-to-consumer flow before Spark Structured Streaming becomes the main consumer.
