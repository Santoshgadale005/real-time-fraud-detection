# Day 17 Summary Report: Advanced Spark Structured Streaming & Fault Tolerance

**Project**: Real-Time Financial Fraud Detection Pipeline  
**Date**: July 11, 2026  
**Week**: Week 3 — Day 17

---

## 1. Objectives Completed

| # | Objective | Status |
|---|-----------|--------|
| 1 | Configure dedicated checkpoint directory (`checkpoints/streaming/`) | ✅ |
| 2 | Implement watermark on `event_time` (10-second late-data tolerance) | ✅ |
| 3 | Attach `FraudStreamingListener` for per-batch metrics | ✅ |
| 4 | Schema validation at every pipeline stage via `assert_schema()` | ✅ |
| 5 | Heartbeat status logging every 30 seconds | ✅ |
| 6 | Graceful shutdown on `KeyboardInterrupt` | ✅ |
| 7 | Create `spark/monitor.py` with listener, schema validator, and status printer | ✅ |
| 8 | Update `spark/streaming.py` with all Day 17 fault-tolerance additions | ✅ |
| 9 | End-to-end pipeline test (32 columns including `event_time`) | ✅ |

---

## 2. Files Created & Updated

| File | Action | Description |
|------|--------|-------------|
| `spark/monitor.py` | **Created** | `FraudStreamingListener` logging batch metrics (batch ID, rows, latency, throughput). `assert_schema()` for column validation. `print_query_status()` for heartbeat logging. |
| `spark/streaming.py` | **Updated** | Watermark on `event_time`, dedicated checkpoint path, listener attachment, schema validation, 30-second heartbeat loop. |
| `checkpoints/streaming/` | **Created** | Checkpoint directory for fault-tolerant offset tracking. Allows Spark to resume from the last committed offset after a crash. |
| `day_17_summary.md` | **Created** | This completion report. |

---

## 3. Fault Tolerance Architecture

```
Kafka Broker
     ↓
Spark readStream (kafka format, latest offsets)
     ↓
Add event_time + Watermark (10 seconds)     ← NEW Day 17
     ↓
Filter invalid records
     ↓
Feature Engineering (5 features)
     ↓
One-Hot Encode transaction type
     ↓
Scale features (StandardScaler parameters)
     ↓
Schema Validation ← assert_schema()         ← NEW Day 17
     ↓
Console Sink (append mode, 5s trigger)
     ↓
Checkpoint commits to checkpoints/streaming/ ← NEW Day 17
```

---

## 4. FraudStreamingListener — Per-Batch Metrics

For every micro-batch, the listener logs:

```
📦 Batch #1 | input_rows=42 | process_ms=312 | trigger_ms=5000 |
             cumulative_rows=42 | throughput=8.4 rows/sec
```

Metrics captured:
- **Batch ID**: monotonically increasing batch number
- **Input rows**: number of Kafka records in this micro-batch
- **Process ms**: time spent executing the Spark DAG (the `addBatch` phase)
- **Trigger ms**: total trigger execution time (includes scheduling overhead)
- **Cumulative rows**: total records processed since job start
- **Throughput**: rolling rows/second estimate

---

## 5. Watermark Configuration

```python
watermarked_df = (
    raw_df
    .withColumn("event_time", to_timestamp(col("timestamp")))
    .withWatermark("event_time", "10 seconds")
)
```

| Setting | Value | Purpose |
|---------|-------|---------|
| Watermark column | `event_time` | Parsed from the producer's UTC `timestamp` field |
| Watermark threshold | `10 seconds` | Tolerate up to 10 seconds of late-arriving data |
| Output mode | `append` | Only emit rows once the watermark passes their window |

---

## 6. Checkpoint Configuration

```python
CHECKPOINT_DIR = PROJECT_ROOT / "checkpoints" / "streaming"
query = final_df.writeStream
    .option("checkpointLocation", str(CHECKPOINT_DIR))
    .trigger(processingTime="5 seconds")
    .start()
```

The checkpoint directory stores:
- **offsets/**: the Kafka partition offsets committed for each micro-batch
- **commits/**: records of successfully completed batches
- **metadata**: query configuration and schema information

On restart, Spark reads from the last committed offset, guaranteeing **exactly-once** processing semantics (with idempotent sinks).

---

## 7. Schema Validation Results

```
[Day17-test] ✅ Schema validation PASSED — all 10 feature columns present.
✅ Day 17 pipeline validation PASSED
   Total columns: 32
   event_time present: True
```

---

## 8. Output Mode Comparison

| Output Mode | When to Use | Used Here? |
|-------------|-------------|-----------|
| `append` | New rows only — no updates (standard streaming) | ✅ Yes |
| `update` | Only changed rows (requires stateful aggregation) | No |
| `complete` | Full result table (requires aggregation) | No |

**Append mode** is correct for our pipeline since each transaction is processed independently (no window aggregation).

---

## 9. Tomorrow's Plan (Day 18)

Day 18 will stress-test the pipeline at high volume, measure throughput and latency benchmarks, tune the trigger interval, and verify checkpoint recovery — all in preparation for ML model integration on Day 19.
