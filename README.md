# Real-Time Financial Fraud Detection Pipeline

An end-to-end real-time fraud detection pipeline simulating a production-grade system.

## Tech Stack
- **Python**
- **Apache Kafka** (Event Streaming)
- **Apache Spark Structured Streaming** (Real-Time Analytics)
- **MongoDB** (Storage of Fraud Alerts)
- **Scikit-Learn (Isolation Forest)** (Anomaly Detection Model)
- **Docker** (Infrastructure Orchestration)
- **Prometheus & Grafana** (Monitoring and Alerting)

## Directory Structure
- `data/`: Raw dataset (PaySim) storage
- `producer/`: Kafka producer simulating continuous transaction stream
- `consumer/`: Kafka consumer utility scripts
- `spark/`: PySpark Structured Streaming logic
- `models/`: ML Model artifacts (Isolation Forest)
- `monitoring/`: Prometheus configuration
- `dashboards/`: Grafana dashboard exports
- `notebooks/`: Jupyter Notebooks for EDA and prototyping
- `config/`: Application environment configurations
- `docker/`: Docker container configuration files
# Real-Time Financial Fraud Detection Pipeline

## Problem Statement

Detect fraudulent financial transactions in real time using Machine Learning and Streaming Technologies.

## Technology Stack

- Python
- Kafka
- Spark Streaming
- FastAPI
- PostgreSQL
- Docker
- MLflow
- AWS
