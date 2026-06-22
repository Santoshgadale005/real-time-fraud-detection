# Day 1 Summary Report: Environment Setup & Infrastructure Initialization

**Project**: Real-Time Financial Fraud Detection Pipeline  
**Date**: June 22, 2026  
**Author**: Santoshgadale005  

---

## 1. Directory Structure Setup
We established a clean, professional project structure in the repository to guarantee a strict separation of concerns between our data, simulation tools, machine learning code, streaming processing engine, and operational dashboards:

```text
real-time-fraud-detection/
├── data/              # PaySim raw data storage
├── producer/          # Kafka transaction simulator
├── consumer/          # Diagnostic consumers
├── spark/             # PySpark Structured Streaming scripts
├── models/            # Serialized Isolation Forest model files
├── monitoring/        # Prometheus metric rules
├── dashboards/        # Grafana dashboards
├── notebooks/         # EDA and prototype notebooks
├── config/            # System configuration variables
├── docker/            # Custom Dockerfiles
├── requirements.txt   # Python dependency manifest
├── docker-compose.yml # Container orchestrations config
├── .env               # Private environment configs (ignored in git)
├── .gitignore         # Version control exclusion rules
└── README.md          # Overview document
```

---

## 2. Python Virtual Environment & Packages
We created a virtual environment (`venv`) to isolate our libraries and dependencies. We installed the following packages:
* **Data Processing & ML**: `pandas`, `numpy`, `scikit-learn` (specifically for our Isolation Forest model).
* **Streaming Engine**: `pyspark` (Structured Streaming) and `kafka-python` (broker connection).
* **Storage Interface**: `pymongo` (NoSQL connectivity).
* **Visualization & Utilities**: `matplotlib`, `seaborn`, `jupyter`, and `python-dotenv`.

---

## 3. Infrastructure Deployment (Docker Compose)
We orchestrated a 4-container local stack utilizing Docker Compose to bypass manual service configuration:
1. **Zookeeper**: Coordinates broker nodes and stores Kafka metadata state.
2. **Kafka**: Event streaming engine running on host port `9092` and internal container port `29092`.
3. **MongoDB**: Secure document warehouse mapping port `27017` to store scored events and alerts.
4. **Kafka UI**: A developer console running on `http://localhost:8080` to inspect topics and verify stream flow.

---

## 4. Key Engineering Resolutions
* **Port Conflict Solved**: Discovered that a native `mongodb-community@7.0` service was running on the macOS host and listening on `27017`. We stopped the native service via `brew services stop` to let Docker bind the port successfully.
* **Image correction**: Updated the Kafka UI container image to `provectuslabs/kafka-ui:latest` to ensure a clean pull from Docker Hub.

---

## 5. Git and Environment Verification
* **Git Status**: Initialized Git on branch `main` and made our first commit. Commits are configured using:
  * **Name**: `Santoshgadale005`
  * **Email**: `santoshgadale005@users.noreply.github.com`
* **Programmatic Health Checks**: Ran the validation script `verify_infra.py` which successfully confirmed TCP socket connectivity and credentials for both MongoDB and Kafka.
