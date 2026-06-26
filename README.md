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
Spark Structured Streaming
    ↓
Fraud Detection Model
    ↓
MongoDB Fraud Alerts
    ↓
Grafana Dashboard
```

## Tech Stack

- **Python**: producer, feature engineering, model development
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
├── consumer/           # Kafka consumer utilities
├── dashboards/         # Grafana dashboard exports
├── data/               # PaySim raw and engineered datasets
├── docker/             # Custom Docker assets
├── docs/               # Architecture, workflow, and data documentation
├── models/             # Feature engineering and model artifacts
├── monitoring/         # Prometheus and monitoring configuration
├── notebooks/          # EDA and prototyping notebooks
├── producer/           # Kafka transaction producer
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

The Python producer reads shared settings from:

```text
config/kafka_config.py
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

## Run the Transaction Producer

Install dependencies if needed:

```bash
pip install -r requirements.txt
```

Create the topic automatically and send 10 test transactions:

```bash
python3 producer/producer.py --create-topic --max-records 10 --delay 0.1
```

Stream the full PaySim dataset:

```bash
python3 producer/producer.py
```

Useful options:

```text
--create-topic       Create the Kafka topic before publishing
--max-records 10     Send only 10 records for a quick test
--delay 0.1          Wait 0.1 seconds between messages
--dataset PATH       Use a different CSV file
```

## Verify Messages in Kafka UI

Open:

```text
http://localhost:8080
```

Navigate to:

```text
Topics -> transactions -> Messages
```

Expected result:

```text
PaySim transaction records appear as JSON messages.
```

## Current Progress

- **Day 1**: Environment setup and Docker infrastructure
- **Day 2**: PaySim dataset generation and fraud EDA
- **Day 3**: Fraud pattern investigation and feature engineering
- **Day 4**: Kafka fundamentals and transaction producer

## Next Step

## 🚀 Day 5 Highlights

Implemented a real-time fraud detection pipeline using Apache Kafka.

### Features Completed

- Kafka Producer publishes transaction data.
- Kafka Consumer receives transaction data in real time.
- Trained Random Forest model predicts fraud.
- Label Encoder converts transaction types.
- Real-time fraud prediction with confidence score.
  
  ## 📊 Sample Prediction

Transaction Received

Prediction: Legitimate

Confidence: 0.9987
