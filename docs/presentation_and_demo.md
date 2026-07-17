# Project Presentation & Demo Script

**Project**: Real-Time Financial Fraud Detection Pipeline

---

## Slide 1: Title
**Title**: Real-Time Financial Fraud Detection Pipeline
**Subtitle**: High-Throughput Event Streaming & ML Inference with Kafka, Spark, and MongoDB
**Presenter**: Santosh Gadale

## Slide 2: The Problem
- **Context**: Financial institutions lose billions to fraud annually.
- **Requirement**: Traditional batch processing is too slow. Fraud must be detected in milliseconds.
- **Challenges**:
  - High volume of concurrent card transactions.
  - Imbalanced class distributions (0.13% fraud rate).
  - High availability & failover demands.

## Slide 3: The Architecture
- **Producer**: PaySim transaction simulator sending events to Kafka.
- **Ingestion**: Apache Kafka buffering event streams.
- **Streaming Engine**: Apache Spark Structured Streaming processing micro-batches.
- **ML Engine**: Unsupervised Isolation Forest model detecting anomalies.
- **Sink**: MongoDB storing fraud alerts (HIGH/MEDIUM/LOW severity).
- **Monitoring**: Grafana dashboards tracking system health & fraud statistics.

## Slide 4: ML Model & Optimization
- **Algorithm**: Isolation Forest (Unsupervised) trained on PaySim.
- **Features**: Time-delta, transaction volume ratio, account drained flags.
- **Optimization**: Grid search improved fraud recall by 3x (from 3.8% to 11.5%) while maintaining a 99% baseline accuracy.

## Slide 5: Performance & Benchmarks
- **Max Throughput**: 4,200 transactions/second.
- **Average Latency**: ~0.6 ms per row inference.
- **Reliability**: Zero message loss across restarts via Spark streaming checkpoints and Kafka offset commits.

---

# Demo Script

1. **Infrastructure Up**:
   ```bash
   docker compose up -d
   ```
2. **Start Python Producer**:
   ```bash
   python3 producer/producer.py --continuous --delay 0.05
   ```
3. **Start Spark Streaming Pipeline**:
   ```bash
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark/streaming.py
   ```
4. **Open Dashboards**:
   - Access Grafana at `http://localhost:3000` (or visual dashboard configs).
   - Show live "Transaction Velocity" and "Fraud Alerts/hour".
5. **Simulate Outage**:
   - Stop Spark Streaming.
   - Wait 15 seconds.
   - Restart Spark Streaming.
   - Demonstrate recovery and offset catching up.
