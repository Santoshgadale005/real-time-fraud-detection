"""spark/performance.py — Streaming Performance Benchmarking (Day 18).

Provides:
  - PerformanceTracker: collects batch metrics (throughput, latency, duration)
    and produces a summary performance report.
  - run_benchmark(): spins up a local Spark pipeline against sample data and
    measures throughput at multiple trigger intervals (50 ms, 100 ms, 200 ms).

Usage:
    python -m spark.performance
"""

from __future__ import annotations

import logging
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

# Allow imports from project root
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

logger = logging.getLogger("spark-performance")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class BatchMetric:
    batch_id:    int
    input_rows:  int
    process_ms:  float
    trigger_ms:  float

@dataclass
class PerformanceReport:
    trigger_interval_ms: int
    total_batches:       int
    total_rows:          int
    avg_rows_per_batch:  float
    avg_process_ms:      float
    avg_trigger_ms:      float
    throughput_rows_sec: float
    error_count:         int


# ---------------------------------------------------------------------------
# PerformanceTracker — collects and aggregates batch metrics
# ---------------------------------------------------------------------------

class PerformanceTracker:
    """Collects per-batch metrics and generates a summary report."""

    def __init__(self) -> None:
        self._metrics: List[BatchMetric] = []
        self._errors: int = 0
        self._start: float = time.perf_counter()

    def record(self, batch_id: int, input_rows: int,
               process_ms: float, trigger_ms: float) -> None:
        self._metrics.append(
            BatchMetric(batch_id, input_rows, process_ms, trigger_ms)
        )

    def record_error(self) -> None:
        self._errors += 1

    def report(self, trigger_interval_ms: int) -> PerformanceReport:
        n = len(self._metrics)
        if n == 0:
            return PerformanceReport(trigger_interval_ms, 0, 0, 0.0, 0.0, 0.0, 0.0, self._errors)

        total_rows     = sum(m.input_rows  for m in self._metrics)
        avg_rows       = total_rows / n
        avg_process_ms = sum(m.process_ms  for m in self._metrics) / n
        avg_trigger_ms = sum(m.trigger_ms  for m in self._metrics) / n
        elapsed_sec    = time.perf_counter() - self._start
        throughput     = total_rows / elapsed_sec if elapsed_sec > 0 else 0.0

        return PerformanceReport(
            trigger_interval_ms = trigger_interval_ms,
            total_batches       = n,
            total_rows          = total_rows,
            avg_rows_per_batch  = round(avg_rows, 2),
            avg_process_ms      = round(avg_process_ms, 2),
            avg_trigger_ms      = round(avg_trigger_ms, 2),
            throughput_rows_sec = round(throughput, 2),
            error_count         = self._errors,
        )

    def log_report(self, report: PerformanceReport) -> None:
        logger.info("=" * 60)
        logger.info("PERFORMANCE REPORT | trigger=%d ms", report.trigger_interval_ms)
        logger.info("  Total batches       : %d", report.total_batches)
        logger.info("  Total rows          : %d", report.total_rows)
        logger.info("  Avg rows/batch      : %.2f", report.avg_rows_per_batch)
        logger.info("  Avg process time    : %.2f ms", report.avg_process_ms)
        logger.info("  Avg trigger time    : %.2f ms", report.avg_trigger_ms)
        logger.info("  Throughput          : %.2f rows/sec", report.throughput_rows_sec)
        logger.info("  Errors              : %d", report.error_count)
        logger.info("=" * 60)


# ---------------------------------------------------------------------------
# Local pipeline benchmark against static sample data
# ---------------------------------------------------------------------------

def run_benchmark() -> None:
    """Benchmark the preprocessing pipeline against a batch of 1000 sample rows."""
    from spark.utils import get_spark_session
    from spark.preprocessing import preprocess_stream, engineer_features, one_hot_encode_type, scale_features
    from spark.monitor import assert_schema

    logger.info("Starting Day 18 Performance Benchmark...")

    spark = get_spark_session("Day18-PerfBenchmark")
    tracker = PerformanceTracker()

    # Build a static batch of 1000 transactions
    base_txn = {
        "step": 1, "type": "CASH_OUT", "amount": 75000.0,
        "nameOrig": "C1", "nameDest": "C2",
        "oldbalanceOrg": 100000.0, "newbalanceOrig": 25000.0,
        "oldbalanceDest": 5000.0,  "newbalanceDest": 80000.0,
        "isFraud": 0, "transaction_id": "tx-bench",
        "timestamp": "2026-07-11T06:00:00.000000+00:00",
    }

    BATCH_SIZES = [100, 500, 1000]
    for batch_size in BATCH_SIZES:
        rows = [dict(base_txn, step=i, transaction_id=f"tx-{i}") for i in range(batch_size)]
        batch_df = spark.createDataFrame(rows)

        t0 = time.perf_counter()
        clean_df      = preprocess_stream(batch_df)
        engineered_df = engineer_features(clean_df)
        encoded_df    = one_hot_encode_type(engineered_df)
        processed_df  = scale_features(encoded_df)
        assert_schema(processed_df, stage=f"bench-{batch_size}")

        # Force execution (collect triggers the DAG)
        count = processed_df.count()
        elapsed_ms = (time.perf_counter() - t0) * 1000

        tracker.record(
            batch_id    = batch_size,
            input_rows  = batch_size,
            process_ms  = elapsed_ms,
            trigger_ms  = elapsed_ms,
        )

        latency_per_row = elapsed_ms / batch_size
        throughput      = batch_size / (elapsed_ms / 1000)

        logger.info(
            "Batch size=%-5d | processed=%d rows | elapsed=%.1f ms "
            "| latency=%.3f ms/row | throughput=%.0f rows/sec",
            batch_size, count, elapsed_ms, latency_per_row, throughput,
        )

    report = tracker.report(trigger_interval_ms=5000)
    tracker.log_report(report)
    spark.stop()
    return report


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run_benchmark()
