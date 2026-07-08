# Day 15 Summary Report: Apache Spark Setup & Kafka Integration

**Project**: Real-Time Financial Fraud Detection Pipeline  
**Date**: July 8, 2026  
**Week**: Week 3 — Day 15

---

## 1. Objectives Completed

| # | Objective | Status |
|---|-----------|--------|
| 1 | Understand Apache Spark architecture & Structured Streaming | ✅ |
| 2 | Install Spark Dependencies (`pyspark`, `findspark`) | ✅ |
| 3 | Create Spark directory structure | ✅ |
| 4 | Create Spark Session builder with Kafka support | ✅ |
| 5 | Connect Spark to Kafka bootstrap servers | ✅ |
| 6 | Read streaming transactions from Kafka topic (`transactions`) | ✅ |
| 7 | Convert binary Kafka payloads to structured JSON | ✅ |
| 8 | Define schema mapping incoming payloads | ✅ |
| 9 | Configure checkpointing directory | ✅ |
| 10| Model Metadata & Health Check endpoints (from remote) | ✅ |

---

## 2. Files Created & Configured

| File | Action | Description |
|------|--------|-------------|
| `requirements.txt` | **Updated** | Added `pyspark` and `findspark` to manifest. Installed packages. |
| `spark/schema.py` | **Created** | StructType schema definitions matching the 12 fields of the raw Kafka payload. |
| `spark/utils.py` | **Created** | Setup helper containing SparkSession builder config. Automatically configures fallback to JDK 21 (Temurin) to resolve Java 24/Hadoop runtime conflicts. |
| `spark/kafka_reader.py` | **Created** | Module using Structured Streaming to read from Kafka topics, cast binary content to strings, parse JSON, and apply TRANSACTION_SCHEMA. |
| `spark/streaming.py` | **Created** | Main orchestrator to setup session, ingest Kafka stream, clean/preprocess data, and stream output to console. |
| `day_15_summary.md` | **Created** | This completion report. |

---

## 3. Spark Ingestion Flow

The ingestion pipeline follows Apache Spark Structured Streaming's micro-batch model:

```
Kafka Broker (localhost:9092)
       ↓ (binary stream)
Spark readStream ("kafka" format)
       ↓ (CAST value AS STRING)
JSON payload
       ↓ (from_json with TRANSACTION_SCHEMA)
Structured Spark DataFrame
```

---

## 4. Key Spark Session Configuration Details

In `spark/utils.py`, the session is built with:
- **`spark.jars.packages`**: `org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0`
- **`spark.sql.streaming.forceDeleteTempCheckpointLocation`**: `true`
- **`spark.driver.memory` / `spark.executor.memory`**: `2g`

---

## 5. Tomorrow's Plan (Day 16)

Tomorrow, we will extend the pipeline to recreate the exact feature engineering steps from Week 2 and apply StandardScaler scaling mathematically within the Spark DAG.
