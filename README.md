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

- **Day 1**: Environment setup and Docker infrastructure ✅
- **Day 2**: PaySim dataset generation and fraud EDA ✅
- **Day 3**: Fraud pattern investigation and feature engineering ✅
- **Day 4**: Kafka fundamentals and transaction producer ✅
- **Day 5**: Kafka consumer and end-to-end streaming validation ✅
- **Day 6**: Continuous producer, UUIDs, timestamps, batching, compression, retries ✅
- **Day 7**: Streaming pipeline validation, throughput and lag tracking, infrastructure integration testing, and project layout cleanup ✅
- **Day 8**: Historical dataset preparation, cleaning, categorical encoding, scaling, and train-test split ✅
- **Day 9**: Isolation Forest unsupervised model training, anomaly predictions and scoring, and model serialization ✅
- **Day 10**: Initial model evaluation, metrics calculation, confusion matrix generation, and baseline performance report ✅
- **Day 11**: False Negative analysis, Isolation Forest hyperparameter grid search (20 experiments), best model selection, and optimization documentation ✅
- **Day 12**: Final model comparison, validation, production model selection, feature contribution analysis, and deployment preparation ✅
- **Day 13**: Production validation, reusable prediction module, end-to-end inference pipeline, deployment artifact packaging, and feature validation checks ✅
- **Day 14**: Model serialization, versioning, deployment preparation, and Spark-ready service package ✅
- **Day 15**: Apache Spark setup, Kafka connection integration, binary payload schema parsing, and Structured Streaming console sinks ✅
- **Day 16**: Spark Structured Streaming data processing, feature engineering recreation, mathematical StandardScaler scaling, and validation parity tests ✅
- **Day 17**: Advanced Spark Structured Streaming, 10s watermark configuration, checkpoints/streaming path setup, FraudStreamingListener metrics logging, and fault recovery ✅
- **Day 18**: Streaming pipeline performance benchmarking (throughput/latency metrics tracker), backpressure optimization, checkpoint recovery validation, and performance reporting ✅

## Day 7 Additions: Throughput and Consumer Lag Monitoring

### Throughput Tracking
Both the Python Producer and Consumer now track real-time throughput. Every 60 seconds of processing, they calculate and log the message rate (transactions per minute) and overall status.
- **Producer Log format**: `Throughput: X.XX transactions/min (Total sent: Y)`
- **Consumer Log format**: `Throughput: X.XX transactions/min  |  Consumer Lag: Y messages  |  Total consumed: Z`

### Consumer Lag Detection
The consumer queries partition offsets programmatically from the Kafka broker to compute the consumer group lag (`log_end_offset - current_position`).
This helps monitor how well the consumer is keeping pace with the producer in real-time, which is crucial for operational stability.

## Week 1 Retrospective & Architecture Review

At the end of Week 1, the pipeline architecture consists of:
```text
[PaySim Historical CSV]
        ↓ (Read by pandas)
[Python Producer (continuous simulator, batching/compression, uuid/timestamp enriched)]
        ↓ (TCP / JSON serialize)
[Kafka Topic: transactions (single partition, confluent-kafka broker)]
        ↓ (TCP / JSON deserialize)
[Python Consumer (group tracking, auto-commit, throughput/lag logging)]
```

All Docker-managed resources (Zookeeper, Kafka, MongoDB, Kafka-UI) are fully verified and integrated.

## Week 2: Historical Data Preparation & Model Training

### Day 8: Data Preprocessing Pipeline
We developed a modular, reusable preprocessing script at `models/training/preprocess.py` to prepare historical data for model training.

#### Preprocessing Steps:
1. **Load Data**: Reads `data/historical/paysim.csv` in chunks/entirety.
2. **Data Cleaning**:
   - Drops duplicate rows.
   - Removes transaction identifier/rule columns that can cause overfitting: `nameOrig`, `nameDest`, and `isFlaggedFraud`.
3. **Categorical Encoding**:
   - Encodes categorical `type` column using dummy encoding with drop-first strategy (`pd.get_dummies(..., drop_first=True)`).
   - Generates numeric dummy columns: `type_CASH_OUT`, `type_DEBIT`, `type_PAYMENT`, `type_TRANSFER`.
4. **Feature Scaling**:
   - Fits `StandardScaler` from scikit-learn on input features `X`.
   - Saves the fitted scaler to `models/scaler.pkl`. (The same scaler is reused during real-time streaming feature scaling).
5. **Stratified Split**:
   - Performs a train-test split (80% training, 20% testing) with stratified distribution to handle extreme class imbalance (0.13% fraud cases).
6. **Export processed files**:
   - Saves final preprocessed datasets to `data/processed/` as:
     - `X_train.csv` (80,000 samples)
     - `X_test.csv` (20,000 samples)
     - `y_train.csv`
     - `y_test.csv`
## Historical Data Preprocessing

The preprocessing pipeline performs:

- Dataset validation
- Duplicate removal
- Missing value handling
- Label encoding of transaction type
- Removal of high-cardinality identifier columns
- Saving processed dataset for model training

Output:
data/processed/processed_paysim.csv

### Day 9: Anomaly Detection with Isolation Forest
We created a modular, reusable model training script at `models/training/train_isolation_forest.py` to train an unsupervised Isolation Forest anomaly detection model on historical data.

Run training:

```bash
venv/bin/python3 models/training/train_isolation_forest.py
```

#### Isolation Forest Parameters:
* `contamination`: `0.001` (representing expected ratio of anomalies/fraud cases)
* `n_estimators`: `100` (number of trees to build in the forest)
* `random_state`: `42` (ensures reproducible results)

#### Prediction Process:
* Unsupervised fitting is performed on `X_train.csv`.
* Prediction outputs of `1` (normal) and `-1` (anomaly) are mapped to standard binary labels: `1` for anomaly/fraud, `0` for normal.
* Decision function scores are generated to provide an anomaly score: more negative values denote highly suspicious anomalies.

#### Generated Artifacts:
* **Trained model binary**: `models/isolation_forest.pkl` (loaded dynamically in Week 3 streaming jobs)
* **Model Metadata JSON**: `models/model_info.json`
* **Output test predictions**: `data/results/fraud_predictions.csv`

#### Baseline Output:
* Training rows: `80,000`
* Test rows: `20,000`
* Predicted anomalies/frauds: `16`
* Actual frauds in test set: `26`
* Next step: evaluate precision, recall, F1 score, and confusion matrix on Day 10.

### Day 10: Initial Model Evaluation & Performance Analysis
We created a reusable evaluation module at `models/evaluation/evaluate_model.py` to measure the Isolation Forest baseline using fraud-focused classification metrics.

Run evaluation:

```bash
venv/bin/python models/evaluation/evaluate_model.py
```

#### Evaluation Metrics:
* `accuracy`: `0.998000`
* `precision`: `0.062500`
* `recall`: `0.038462`
* `f1_score`: `0.047619`

#### Confusion Matrix:

| Actual / Predicted | Normal | Fraud |
|--------------------|--------|-------|
| Actual Normal | `19959` | `15` |
| Actual Fraud | `25` | `1` |

#### Generated Evaluation Artifacts:
* **Metrics CSV**: `data/results/model_metrics.csv`
* **Confusion matrix image**: `data/results/confusion_matrix.png`
* **Evaluation summary**: `docs/evaluation_summary.md`

### Day 11: False Negative Analysis & Isolation Forest Optimization
We built a systematic optimization pipeline at `models/optimization/optimize_model.py` that analyses missed fraud transactions and runs a hyperparameter grid search across 20 Isolation Forest configurations.

Run optimization:

```bash
venv/bin/python models/optimization/optimize_model.py
```

#### Hyperparameter Search Grid:
| Parameter | Values Tested |
|-----------|---------------|
| `contamination` | `0.001`, `0.002`, `0.005`, `0.010` |
| `n_estimators` | `100`, `200`, `300` |
| `max_samples` | `auto`, `10000`, `50000` |
| `max_features` | `1.0`, `0.8`, `0.6` |

#### Best Model Configuration (M18):
* `contamination`: `0.005`
* `n_estimators`: `300`
* `max_samples`: `50000`
* `max_features`: `1.0`

#### Baseline vs Optimized Performance:
| Metric | Baseline | Optimized |
|--------|----------|-----------|
| Recall | `0.038462` | **`0.115385`** (3× improvement) |
| False Negatives | `25` | **`23`** (-2 missed frauds) |
| True Positives | `1` | **`3`** |
| False Positives | `15` | `99` |

#### Generated Optimization Artifacts:
* **Best model**: `models/best_isolation_forest.pkl`
* **Comparison table**: `data/results/model_comparison.csv`
* **Optimized confusion matrix**: `data/results/optimized_confusion_matrix.png`
* **Score distribution plot**: `data/results/optimization_plots/false_negative_score_distribution.png`
* **Recall vs contamination plot**: `data/results/optimization_plots/recall_by_contamination.png`
* **Optimization report**: `docs/model_optimization.md`

### Day 12: Final Model Comparison, Validation & Production Model Selection
We built a comprehensive model comparison pipeline at `models/comparison/compare_models.py` that evaluates the Baseline vs Optimized Isolation Forest, validates stability, measures performance, and selects the production-ready model.

Run comparison:

```bash
venv/bin/python models/comparison/compare_models.py
```

#### Performance Comparison:
| Metric | Baseline | Optimized | Change |
|--------|----------|-----------|--------|
| Accuracy | `0.998000` | `0.993900` | -0.004100 |
| Precision | `0.062500` | `0.029412` | -0.033088 |
| Recall | `0.038462` | **`0.115385`** | **+0.076923 (3× improvement)** |
| F1 Score | `0.047619` | `0.046875` | ≈same |
| False Negatives | `25` | **`23`** | **-2 fewer missed** |
| True Positives | `1` | **`3`** | **+2 more caught** |

#### Production Model Configuration:
| Parameter | Value |
|-----------|-------|
| `contamination` | `0.005` |
| `n_estimators` | `300` |
| `max_samples` | `50000` |
| `max_features` | `1.0` |
| `random_state` | `42` |

#### Processing Performance:
| Metric | Value |
|--------|-------|
| Training time | `1.41` seconds |
| Per-transaction inference | `0.011` ms |
| Throughput | **89,681 transactions/second** |

#### Generated Day 12 Artifacts:
* **Production model**: `models/production_model.pkl`
* **Production config**: `models/production_model_config.json`
* **Performance comparison plot**: `data/results/comparison_plots/performance_comparison.png`
* **Confusion matrix comparison**: `data/results/comparison_plots/confusion_matrices_comparison.png`
* **Feature contribution plot**: `data/results/comparison_plots/feature_contribution.png`
* **Final comparison CSV**: `data/results/final_model_comparison.csv`
* **Final evaluation report**: `docs/final_model_report.md`

### Day 13: Production Validation & Deployment Readiness

We built the complete inference pipeline, packaged all deployment artifacts, and ran a
9-step end-to-end validation to confirm the model is ready for Spark Structured Streaming.

Run validation:

```bash
venv/bin/python deployment/scripts/validate_pipeline.py
```

#### Final Model Selected

**Optimized Isolation Forest v1.0.0** (`models/production_model.pkl`)

| Parameter | Value |
|-----------|-------|
| `contamination` | `0.005` |
| `n_estimators` | `300` |
| `max_samples` | `50000` |
| `random_state` | `42` |

#### Evaluation Summary

| Metric | Value |
|--------|-------|
| Accuracy | `0.9939` |
| Precision | `0.0294` |
| Recall | `0.1154` (3× baseline) |
| F1 Score | `0.0469` |
| False Negatives | `23` (vs 25 baseline) |
| Throughput | **89,681 txn/sec** |

#### Prediction Workflow

```
Transaction Dictionary
        ↓
validate_features()   — checks missing/extra/wrong-type columns
        ↓
Feature Ordering      — enforces EXPECTED_FEATURES order
        ↓
scaler.transform()    — StandardScaler (same as training)
        ↓
model.predict()       — Isolation Forest anomaly detection
        ↓
Output: { prediction, anomaly_score, is_fraud, label }
```

#### Deployment Artifacts

| Artifact | Path |
|----------|------|
| Production model | `models/production_model.pkl` |
| Fitted scaler | `models/scaler.pkl` |
| Prediction module | `models/predict.py` |
| Deployment config | `deployment/config/model_config.json` |
| Packaged artifacts | `deployment/artifacts/` |
| Validation script | `deployment/scripts/validate_pipeline.py` |
| Deployment README | `deployment/README.md` |

#### Day 13 Validation Results

```
✅ PASSED  Artifact Loading
✅ PASSED  Normal Transaction Prediction
✅ PASSED  Fraud Transaction Prediction
✅ PASSED  Batch Predictions (100 / 500 / 1000)
✅ PASSED  Prediction Consistency (deterministic)
✅ PASSED  Inference Latency
✅ PASSED  Feature Order Validation
✅ PASSED  Feature Validation Function
✅ PASSED  Full Pipeline Validation

🎉 9/9 STEPS PASSED
```
## Monitoring Dashboard

Features:
- Streamlit dashboard
- Live prediction analytics
- KPI cards
- Fraud rate monitoring
- Confidence distribution
- Prediction trends
- API health endpoint

## Model Metadata API

### GET /metadata

Returns the deployed model information.

Example Response

```json
{
  "model_name": "Real-Time Fraud Detection",
  "version": "1.0.0",
  "algorithm": "Random Forest",
  "trained_on": "2026-07-08",
  "accuracy": 0.9989,
  "status": "Production"
}
```

## Health Check

### GET /health

```json
{
  "status": "healthy",
  "model_loaded": true,
  "version": "1.0.0"
}
```

### Day 15: Apache Spark Setup & Kafka Integration

We integrated PySpark Structured Streaming with our Kafka broker:
- Configured a local Spark Session running with `spark-sql-kafka-0-10_2.13:3.5.0` dependencies.
- Defined a structured transaction schema `TRANSACTION_SCHEMA` mirroring the 12 fields of raw PaySim transactions.
- Created `spark/kafka_reader.py` to ingest, cast, and deserialize incoming JSON streams.

### Day 16: Structured Streaming Data Processing & Feature Pipeline

We built a high-performance feature preprocessing pipeline in PySpark (`spark/preprocessing.py`):
- Filtered out invalid transactions (e.g., negative amounts or null fields).
- Recreated engineered features (`origin_balance_diff`, `dest_balance_diff`, `amount_balance_ratio`, `account_drained`, `high_value_txn`).
- Created one-hot dummy variables for categorical types (`type_CASH_OUT`, `type_DEBIT`, `type_PAYMENT`, `type_TRANSFER`).
- Loaded the fitted StandardScaler parameter dictionary (`scaler_v1.pkl`) and mathematically scaled streaming batches on JVM.
- Validated mathematical parity via `spark/test_pipeline.py`, yielding **100% agreement** between scikit-learn standard scaling and Spark scaling.

### Day 17: Advanced Spark Structured Streaming & Fault Tolerance

We implemented fault-tolerant query handling and execution tracking:
- **Watermarking**: Configured a `10-second` watermark on the `event_time` column (parsed from the incoming payload timestamp) to manage late data arrival.
- **Checkpointing**: Configured state checkpoints directed to `checkpoints/streaming/` to save execution offsets and query configurations.
- **Telemetry & Monitoring**: Created a `FraudStreamingListener` that attaches directly to the SparkSession, tracking input rates, batch processing durations, and trigger execution times.
- **Heartbeat & Graceful Shutdown**: Added a 30-second status check loop to monitor active queries and safely release listener hooks on process termination.

### Day 18: Streaming Pipeline Validation & Performance Optimization

We validated and optimized the streaming feature pipeline under variable loads:
- **Automated Performance Test**: Developed `spark/performance.py` implementing automated latency and throughput benchmarks.
- **Throughput Metrics**: Measured peak processing throughput at **6,773 rows/second** at a batch size of 1000, with a sub-millisecond per-row latency of **0.148 ms/row**.
- **Crash Recovery Validation**: Verified that Spark successfully reads intermediate checkpoint logs and resumes consuming Kafka messages from the last committed offset on restart.

