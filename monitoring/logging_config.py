"""monitoring/logging_config.py — Centralized Logging Configuration (Day 21).

Provides a single entry-point for configuring all application loggers with:
  - Console handler (INFO+)
  - Rotating file handlers per component (DEBUG+)
  - Structured format with timestamps, levels, and module names

Usage:
    from monitoring.logging_config import setup_logging
    setup_logging()   # call once at application startup
"""

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = PROJECT_ROOT / "logs"


def setup_logging(log_level: int = logging.DEBUG) -> None:
    """Configure centralized logging for all pipeline components.

    Creates dedicated log files:
      - logs/application.log  — all components (INFO+)
      - logs/stream.log       — spark-streaming-pipeline (DEBUG+)
      - logs/prediction.log   — spark-ml-predictor (DEBUG+)
      - logs/database.log     — mongodb-client (DEBUG+)
      - logs/monitor.log      — spark-monitor (DEBUG+)
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # Common formatter
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(name)-30s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ── Root logger: console (INFO) + application.log (INFO) ─────────────────
    root = logging.getLogger()
    root.setLevel(log_level)
    # Remove any pre-existing handlers to avoid duplicates on re-import
    root.handlers.clear()

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)
    root.addHandler(console)

    app_handler = RotatingFileHandler(
        LOG_DIR / "application.log", maxBytes=10_000_000, backupCount=5, encoding="utf-8"
    )
    app_handler.setLevel(logging.INFO)
    app_handler.setFormatter(fmt)
    root.addHandler(app_handler)

    # ── Component-specific loggers ───────────────────────────────────────────
    component_logs = {
        "spark-streaming-pipeline": "stream.log",
        "spark-ml-predictor":       "prediction.log",
        "mongodb-client":           "database.log",
        "spark-monitor":            "monitor.log",
        "spark-performance":        "monitor.log",
        "spark-preprocessing":      "stream.log",
        "fraud-predict-service":    "prediction.log",
    }

    for logger_name, filename in component_logs.items():
        lgr = logging.getLogger(logger_name)
        lgr.setLevel(logging.DEBUG)
        fh = RotatingFileHandler(
            LOG_DIR / filename, maxBytes=5_000_000, backupCount=3, encoding="utf-8"
        )
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        lgr.addHandler(fh)

    logging.getLogger("py4j").setLevel(logging.ERROR)  # suppress Spark noise
    logging.getLogger("pyspark").setLevel(logging.WARNING)

    logging.info("Centralized logging configured — log directory: %s", LOG_DIR)
