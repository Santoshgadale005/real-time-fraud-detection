# 📋 Week 1 & Week 2 — Real-Time Financial Fraud Detection Pipeline: Presentation Pitch

## Executive Summary

- **Week 1**: Built the Docker infrastructure (Kafka, ZooKeeper, MongoDB) and a continuous simulation pipeline. Features custom UUIDs, network compression (gzip), and real-time consumer lag monitoring (`log_end_offset - current_position`).
- **Week 2**: Designed an unsupervised **Isolation Forest** model to detect rare anomalies. Tuned hyperparameters over 20 runs, improving recall **3×** (to 11.5%). Validated latency at **0.011 ms/txn** (89K txn/sec) and packaged versioned binaries into a Spark-ready `predict_service.py`.

---

## 🎤 Presentation Pitch & Demo Flow

### 1. Opening & Problem Statement (45s)
> *"Hello everyone. I'm presenting our Real-Time Financial Fraud Detection Pipeline. In digital payments, fraud changes constantly, and datasets are heavily imbalanced. Traditional rule-based engines miss new strategies. 
> 
> To solve this, I built an end-to-end unsupervised pipeline. It ingests live transaction streams via Kafka and runs a production-grade Isolation Forest model to detect anomalies in real time without requiring historical labels."*

---

### 2. Week 1: Streaming Ingestion Backbone (60s)
> *"In Week 1, I established the streaming backbone. I containerized Zookeeper, Kafka, and MongoDB, then wrote a continuous transaction producer that simulates a live payment gateway using gzip compression and custom UUID metadata.
> 
> Crucially, I implemented a telemetry consumer that monitors real-time group lag programmatically by comparing partition offset bounds. This guarantees our system handles high-throughput flows without dropping messages."*

#### 📂 Files to show for Week 1:
- **[`producer/producer.py`](file:///Users/santoshgadale/Desktop/zaalima%202/real-time-fraud-detection/producer/producer.py)**: Show the continuous transaction simulator loop, UUID generation, and network compression (`gzip`).
- **[`consumer/consumer.py`](file:///Users/santoshgadale/Desktop/zaalima%202/real-time-fraud-detection/consumer/consumer.py)**: Show the loop logging throughput and the programmatic consumer group lag calculation (`log_end_offset - current_position`).
- **[`config/kafka_config.py`](file:///Users/santoshgadale/Desktop/zaalima%202/real-time-fraud-detection/config/kafka_config.py)**: Show environment configuration, topic definitions, and broker targets.

---

### 3. Week 2: Machine Learning & Deployment (60s)
> *"In Week 2, I engineered the machine learning service. I created a robust preprocessing module and trained an unsupervised Isolation Forest. 
> 
> Through a systematic grid search of 20 configurations, I optimized the model, boosting recall by 3× (to 11.5%) while keeping per-transaction latency under 0.011 milliseconds—equivalent to 89,000 transactions per second. 
> 
> Finally, I validated the pipeline through a 9-step test harness and packaged it into a Spark-ready prediction service (`predict_service.py`) for real-time streaming integration."*

#### 📂 Files to show for Week 2:
- **[`models/training/preprocess.py`](file:///Users/santoshgadale/Desktop/zaalima%202/real-time-fraud-detection/models/training/preprocess.py)**: Show the preprocessing function fitting the `StandardScaler` on features and mapping categorical transaction types.
- **[`models/optimization/optimize_model.py`](file:///Users/santoshgadale/Desktop/zaalima%202/real-time-fraud-detection/models/optimization/optimize_model.py)**: Show the grid search loop and parameter configuration settings where recall was tuned and M18 was selected.
- **[`models/predict.py`](file:///Users/santoshgadale/Desktop/zaalima%202/real-time-fraud-detection/models/predict.py)**: Show the reusable prediction function and the input validation checks (`validate_features`).
- **[`deployment/predict_service.py`](file:///Users/santoshgadale/Desktop/zaalima%202/real-time-fraud-detection/deployment/predict_service.py)**: Show the stateless `FraudPredictionService` wrapper optimized for Apache Spark Structured Streaming UDFs.
- **[`deployment/config/features.json`](file:///Users/santoshgadale/Desktop/zaalima%202/real-time-fraud-detection/deployment/config/features.json)**: Show the authoritative feature order contract and PySpark database schema definitions.

---

### 4. Closing (15s)
> *"We now have a validated, low-latency, containerized ML engine ready to consume and score streaming Kafka transactions inside Apache Spark. Thank you, and I'd love to take your questions."*

---

## 💡 Quick Q&A Cheat Sheet

- **Why Isolation Forest?** "Fraud is highly imbalanced (<0.2%) and constantly evolving. Unsupervised isolation lets us flag unseen anomalies without needing historical labels."
- **Why is precision low (2.9%)?** "In fraud, a False Negative (missing fraud) costs significantly more than a False Positive (alert). A 3× recall increase maximizes financial protection, and we can fine-tune the threshold in `model_config.json`."
- **How does Spark integrate it?** "The model and scaler are wrapped in a thread-safe, stateless `FraudPredictionService` class. Spark loads this service inside a distributed UDF to score streaming batches in parallel."
