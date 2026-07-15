"""monitoring/health.py — Pipeline Health Monitor (Day 21).

Provides a HealthMonitor that periodically checks the liveness of each
pipeline component (Kafka, Spark, MongoDB, Model) and exposes a summary.

Also tracks cumulative streaming KPIs:
  - total_transactions_processed
  - total_fraud_alerts
  - total_errors
  - uptime_seconds
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Dict

logger = logging.getLogger("spark-monitor")


@dataclass
class ComponentStatus:
    name: str
    healthy: bool = False
    last_check: float = 0.0
    message: str = ""


@dataclass
class PipelineKPIs:
    """Cumulative key performance indicators tracked across all micro-batches."""
    total_transactions: int = 0
    total_fraud_alerts: int = 0
    total_normal: int = 0
    total_errors: int = 0
    total_batches: int = 0
    total_mongo_writes: int = 0
    start_time: float = field(default_factory=time.time)

    @property
    def uptime_seconds(self) -> float:
        return time.time() - self.start_time

    @property
    def fraud_rate(self) -> float:
        if self.total_transactions == 0:
            return 0.0
        return self.total_fraud_alerts / self.total_transactions

    @property
    def throughput(self) -> float:
        elapsed = self.uptime_seconds
        if elapsed == 0:
            return 0.0
        return self.total_transactions / elapsed

    def summary(self) -> Dict:
        return {
            "uptime_seconds": round(self.uptime_seconds, 1),
            "total_transactions": self.total_transactions,
            "total_fraud_alerts": self.total_fraud_alerts,
            "total_normal": self.total_normal,
            "total_errors": self.total_errors,
            "total_batches": self.total_batches,
            "total_mongo_writes": self.total_mongo_writes,
            "fraud_rate_pct": round(self.fraud_rate * 100, 2),
            "avg_throughput_rps": round(self.throughput, 2),
        }


class HealthMonitor:
    """Tracks component health and pipeline KPIs."""

    def __init__(self) -> None:
        self.kpis = PipelineKPIs()
        self._components: Dict[str, ComponentStatus] = {}

    def register(self, name: str) -> None:
        self._components[name] = ComponentStatus(name=name)

    def mark_healthy(self, name: str, message: str = "OK") -> None:
        if name not in self._components:
            self.register(name)
        self._components[name].healthy = True
        self._components[name].last_check = time.time()
        self._components[name].message = message

    def mark_unhealthy(self, name: str, message: str = "FAIL") -> None:
        if name not in self._components:
            self.register(name)
        self._components[name].healthy = False
        self._components[name].last_check = time.time()
        self._components[name].message = message
        logger.warning("Component '%s' marked UNHEALTHY: %s", name, message)

    def all_healthy(self) -> bool:
        return all(c.healthy for c in self._components.values())

    def log_status(self) -> None:
        logger.info("─── Pipeline Health Status ───")
        for c in self._components.values():
            icon = "✅" if c.healthy else "❌"
            logger.info("  %s %-15s | %s", icon, c.name, c.message)
        kpi = self.kpis.summary()
        logger.info("─── KPIs ───")
        for k, v in kpi.items():
            logger.info("  %-25s : %s", k, v)
        logger.info("─────────────────────────────")
