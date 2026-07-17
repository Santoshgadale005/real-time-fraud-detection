# System Stress Testing & Performance Validation Report

**Date**: July 16, 2026

## 1. Executive Summary
We performed high-volume stress testing on the Real-Time Financial Fraud Detection Pipeline. The objective was to evaluate the system's capacity, stability, and bottleneck limits under load rates ranging from 500 tx/sec up to 5,000 tx/sec.

## 2. Test Configuration & Parameters
- **Baseline Rate**: 100 tx/sec
- **Stress Tiers**: 500 tx/sec, 1,000 tx/sec, 5,000 tx/sec (simulated peak gateway loads)
- **Duration**: 30-minute continuous streaming run per test tier
- **Kafka Partitions**: 3 (increased from 1 for improved consumer-side parallelism)
- **Spark Configuration**: 4GB Driver, 4GB Executor, 10 Shuffle Partitions
- **MongoDB Configuration**: Bulk writes with `ordered=False`

## 3. Benchmark Metrics

| Metric / Metric Tier | Baseline (100 tx/sec) | Medium Load (1,000 tx/sec) | High Load (5,000 tx/sec) |
|---|---|---|---|
| **Avg. Batch Duration (ms)** | 480 ms | 980 ms | 3,120 ms |
| **Throughput (tx/sec)** | 100 tx/sec | 1,000 tx/sec | 4,200 tx/sec (bottleneck limit) |
| **Consumer Lag (messages)** | 0 | < 150 | ~85,000 (accumulating) |
| **Inference Latency (ms/row)** | 4.8 ms | 0.9 ms | 0.6 ms (highly optimized via batching) |
| **MongoDB Write Latency (ms)** | 12 ms | 35 ms | 110 ms |
| **CPU Utilization (Spark)** | 15% | 45% | 85% |
| **Memory Utilization (JVM)** | 2.1 GB | 3.4 GB | 3.8 GB |

## 4. Identified Bottlenecks & Optimization
- **MongoDB Writes**: Above 4,000 tx/sec, MongoDB write latency spikes due to disk I/O constraints. Resolving this requires scale-out of the MongoDB cluster or shard configuration.
- **Consumer Lag**: At 5,000 tx/sec, consumer lag began accumulating because the single-core python ML inference script reached CPU saturation. Parallel execution across multiple Spark executors solved this partially.

## 5. Stability & Recovery Validation
The pipeline ran for 30 minutes without crashes. After shutting down the Spark engine mid-stream under load, checkpoints recovered 100% of uncommitted Kafka offsets on restart.
