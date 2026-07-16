# Day 22 Summary Report: Grafana Dashboard Integration & Real-Time Monitoring

**Project**: Real-Time Financial Fraud Detection Pipeline  
**Date**: July 12, 2026  
**Week**: Week 4 — Day 22

---

## 1. Objectives Completed

| # | Objective | Status |
|---|-----------|--------|
| 1 | Create dashboards config folder `dashboards/` | ✅ |
| 2 | Design and configure the Grafana dashboard panels | ✅ |
| 3 | Implement real-time transaction velocity visualization (processed rows/sec) | ✅ |
| 4 | Implement fraud alert volume panel (MongoDB aggregate count over time) | ✅ |
| 5 | Implement prediction latency comparison (Spark batch duration vs ML latency) | ✅ |
| 6 | Visualize fraud alert severity distribution (HIGH / MEDIUM / LOW donut chart) | ✅ |
| 7 | Visualize alert transaction type distribution (CASH_OUT, TRANSFER, etc. pie chart) | ✅ |
| 8 | Export dashboard structure into `dashboards/fraud_dashboard.json` | ✅ |
| 9 | Document dashboard setups, data sources, and refresh frequency | ✅ |

---

## 2. Files Created & Updated

| File | Action | Description |
|------|--------|-------------|
| `dashboards/fraud_dashboard.json` | **Created** | Grafana dashboard config file. Defines panels connecting to MongoDB (fraud_alerts collection) and Prometheus (streaming metrics). |
| `day_22_summary.md` | **Created** | This completion report. |

---

## 3. Grafana Dashboard Panels & Layout

The dashboard is structured into three logical rows:

### Row 1: System Overview & Status KPIs
- **Total Fraud Alerts (`stat` panel)**: Displays the running cumulative total of transactions flagged as fraudulent (`prediction: 1`).
- **High Risk Alerts (`stat` panel)**: Count of fraud alerts with `severity: 'HIGH'` (anomaly score < -0.8). Displays orange/red thresholds.
- **Fraud Severity Distribution (`donut` chart)**: Displays the ratio of HIGH, MEDIUM, and LOW risk alerts.

### Row 2: Velocity & Latency Performance (Time Series)
- **Transaction Velocity (`timeseries` panel)**: Shows processing throughput in events/sec, using Prometheus `rate(spark_streaming_processed_rows_total[1m])`.
- **System Latency (`timeseries` panel)**: Side-by-side plot comparing total Spark micro-batch duration (`spark_streaming_batch_duration_ms`) against ML inference latency (`spark_streaming_prediction_latency_ms`).

### Row 3: Database Writes & Types
- **MongoDB Fraud Alert Writes (`timeseries` panel)**: Connects directly to the MongoDB `fraud_alerts` collection to count insertions grouped by minute, capturing live database write speeds.
- **Alert Type Distribution (`pie` chart)**: Displays the breakdown of categories of flagged transactions (e.g., TRANSFER vs. CASH_OUT).

---

## 4. Data Source Configuration

The dashboard uses two target data sources:

1. **MongoDB Datasource (`mongodb-datasource`)**:
   - Connection: `mongodb://admin:admin123@mongodb:27017`
   - Database: `fraud_detection`
   - Collection: `fraud_alerts`
   - Query Type: Aggregate pipelines (e.g., grouping alerts by severity or time buckets).
2. **Prometheus Datasource (`prometheus`)**:
   - Connection: `http://prometheus:9090`
   - Purpose: Visualizes operational telemetry exported from Spark Structured Streaming and host system resource monitors.

---

## 5. Dashboard Telemetry Settings

- **Refresh Interval**: Configured to auto-refresh every **5 seconds** for highly responsive visual updates.
- **Default Time window**: Configured to show the last **15 minutes** of active ingestion.
- **Visual Style**: Dark mode optimized theme (`style: "dark"`) with clear thresholds (green/orange/red) on alert KPIs.
