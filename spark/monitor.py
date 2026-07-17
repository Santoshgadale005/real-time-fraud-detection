"""spark/monitor.py — Streaming Query Monitor (Day 17).

Provides a StreamingQueryListener that logs key metrics for every micro-batch:
  - Batch ID
  - Input rows
  - Processing time (ms)
  - Trigger execution time (ms)
  - Batch duration estimate
  - Running throughput (rows/sec)

Also provides helper utilities:
  - print_query_status(): pretty-prints the current query status dict.
  - assert_schema(): validates that a streaming DataFrame has all required columns.
"""

import logging
import time
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQueryListener

logger = logging.getLogger("spark-monitor")

# ---------------------------------------------------------------------------
# Streaming Query Listener
# ---------------------------------------------------------------------------

class FraudStreamingListener(StreamingQueryListener):
    """Attaches to the SparkSession and logs metrics for every micro-batch."""

    def __init__(self) -> None:
        self._batch_count = 0
        self._total_rows  = 0
        self._start_time  = time.time()

    def onQueryStarted(self, event) -> None:  # type: ignore[override]
        logger.info("🟢 Streaming query STARTED | id=%s | name=%s",
                    event.id, event.name)

    def onQueryProgress(self, event) -> None:  # type: ignore[override]
        progress = event.progress
        self._batch_count += 1
        input_rows   = progress.numInputRows
        duration_ms = getattr(progress, "durationMs", {})
        trigger_ms = duration_ms.get("triggerExecution", 0) if isinstance(duration_ms, dict) else 0
        process_ms = duration_ms.get("addBatch", 0) if isinstance(duration_ms, dict) else 0
        self._total_rows += input_rows

        elapsed = time.time() - self._start_time
        throughput = self._total_rows / elapsed if elapsed > 0 else 0.0

        logger.info(
            "📦 Batch #%d | input_rows=%d | process_ms=%d | trigger_ms=%d | "
            "cumulative_rows=%d | throughput=%.1f rows/sec",
            self._batch_count, input_rows, process_ms, trigger_ms,
            self._total_rows, throughput,
        )

        if progress.numInputRows == 0:
            logger.debug("   ⏳ No new data in this micro-batch (idle trigger).")

    def onQueryIdle(self, event) -> None:
        """Called when query is idle."""
        pass

    def onQueryTerminated(self, event) -> None:  # type: ignore[override]
        if event.exception:
            logger.error("🔴 Streaming query TERMINATED with error | id=%s | error=%s",
                         event.id, event.exception)
        else:
            logger.info("🔵 Streaming query TERMINATED cleanly | id=%s", event.id)


# ---------------------------------------------------------------------------
# Schema Validator
# ---------------------------------------------------------------------------

REQUIRED_FEATURE_COLS = [
    "step", "amount", "oldbalanceOrg", "newbalanceOrig",
    "oldbalanceDest", "newbalanceDest",
    "type_CASH_OUT", "type_DEBIT", "type_PAYMENT", "type_TRANSFER",
]

def assert_schema(df: DataFrame, stage: str = "unknown") -> None:
    """Raise ValueError if any required feature column is missing from df.

    Args:
        df:    The streaming DataFrame to check.
        stage: Label for the pipeline stage (used in log messages).
    """
    existing = set(df.columns)
    missing  = [c for c in REQUIRED_FEATURE_COLS if c not in existing]
    if missing:
        raise ValueError(
            f"[{stage}] Schema validation FAILED. Missing columns: {missing}\n"
            f"Available columns: {sorted(existing)}"
        )
    logger.info("[%s] ✅ Schema validation PASSED — all %d feature columns present.",
                stage, len(REQUIRED_FEATURE_COLS))


# ---------------------------------------------------------------------------
# Query Status Pretty-Printer
# ---------------------------------------------------------------------------

def print_query_status(query, label: str = "") -> None:
    """Log the current status of a running streaming query.

    Args:
        query: A pyspark StreamingQuery object.
        label: Optional label prefix for the log line.
    """
    tag = f"[{label}] " if label else ""
    status = query.status
    logger.info(
        "%sQuery status | active=%s | message=%s | data_available=%s | trigger_active=%s",
        tag,
        query.isActive,
        status.get("message", ""),
        status.get("isDataAvailable", False),
        status.get("isTriggerActive", False),
    )
