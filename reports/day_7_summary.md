# Day 7 Summary Report
## Project: Real-Time Fraud Detection System

**Date:** 29 June 2026

---

# Objective

The objective of Day 7 was to containerize the complete Real-Time Fraud Detection System using Docker and Docker Compose, enabling all services to run together as an integrated application.

---

# Tasks Completed

## 1. Dockerized FastAPI Application

- Created FastAPI Dockerfile
- Configured Uvicorn server
- Exposed API on port 8000
- Successfully deployed API inside Docker

---

## 2. Dockerized Kafka Producer

- Created Producer Dockerfile
- Connected Producer with Kafka
- Loaded PaySim dataset
- Published transactions continuously to Kafka topic

---

## 3. Dockerized Kafka Consumer

- Created Consumer Dockerfile
- Loaded trained Random Forest model
- Loaded Label Encoder
- Connected Consumer with Kafka
- Generated real-time fraud predictions

---

## 4. Docker Compose Integration

Integrated the following services:

- Apache Kafka
- Zookeeper
- MongoDB
- Kafka UI
- FastAPI
- Producer
- Consumer

All services were successfully orchestrated using Docker Compose.

---

## 5. Dataset Configuration

Configured PaySim dataset inside Docker containers.

Dataset Path:

data/raw/paysim.csv

---

## 6. Kafka Streaming

Successfully streamed real-time transaction data from Producer to Kafka.

Topic Used:

transactions

---

## 7. Fraud Prediction

Consumer successfully processed incoming Kafka messages.

Example Output:

Prediction: Legitimate

Confidence: 1.0

---

## 8. API Deployment

FastAPI Swagger Documentation:

http://localhost:8000/docs

API successfully deployed inside Docker.

---

## 9. Kafka Monitoring

Kafka UI:

http://localhost:8080

Verified:

- Kafka Cluster
- Topics
- Real-Time Messages
- Producer Activity

---

# Challenges Faced

- Kafka bootstrap timeout
- Missing dataset inside Docker
- Docker networking configuration
- Kafka Consumer initialization
- Scikit-Learn version mismatch (1.3.2 vs 1.7.2)
- Missing Kafka Consumer object
- Docker image rebuilding

---

# Solutions Implemented

- Added dataset into Docker project
- Updated requirements.txt with compatible Scikit-Learn version
- Fixed Kafka Consumer initialization
- Configured Docker Compose networking
- Rebuilt Docker images
- Verified Kafka Producer and Consumer communication

---

# Technologies Used

- Python
- FastAPI
- Docker
- Docker Compose
- Apache Kafka
- MongoDB
- Scikit-Learn
- Joblib
- Pandas

---

# Outcome

Successfully developed a fully Dockerized Real-Time Fraud Detection System capable of:

- Streaming transactions
- Real-time fraud prediction
- Kafka message processing
- API deployment
- Containerized microservices architecture

Day 7 completed successfully.