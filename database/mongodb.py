"""database/mongodb.py — MongoDB Database connection and alerts insertion wrapper (Day 20).

Provides thread-safe connections to MongoDB with automatic write retry mechanism
for storing real-time fraud alerts.
"""

import os
import time
import logging
from typing import Any, Dict, List
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, PyMongoError

logger = logging.getLogger("mongodb-client")

# Load environment variable configuration or fall back to defaults
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin123@localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "fraud_detection")
ALERTS_COLLECTION = os.getenv("MONGO_ALERTS_COLLECTION", "fraud_alerts")


class MongoDBClient:
    """Thread-safe MongoDB client wrapper with automatic write retry logic."""

    def __init__(self, uri: str = MONGO_URI, db_name: str = MONGO_DB) -> None:
        self._uri = uri
        self._db_name = db_name
        self._client = None
        self._db = None
        self._collection = None
        self._connect()

    def _connect(self) -> None:
        """Establish connection to MongoDB."""
        try:
            logger.info("Connecting to MongoDB at: %s", self._uri.split("@")[-1])  # Mask credentials
            self._client = MongoClient(self._uri, serverSelectionTimeoutMS=5000)
            # Trigger serverSelectionTimeoutMS if node is unavailable
            self._client.admin.command("ping")
            self._db = self._client[self._db_name]
            self._collection = self._db[ALERTS_COLLECTION]
            logger.info("✅ Successfully connected to MongoDB database: %s", self._db_name)
        except (ConnectionFailure, PyMongoError) as e:
            logger.error("❌ Failed to connect to MongoDB: %s", e)
            self._client = None
            self._db = None
            self._collection = None

    def _ensure_connected(self) -> bool:
        """Check connection state and attempt reconnect if dropped."""
        if self._client is None or self._collection is None:
            self._connect()
        return self._client is not None

    def insert_alert(self, alert: Dict[str, Any], max_retries: int = 3, backoff: float = 1.0) -> bool:
        """Insert a single fraud alert document into MongoDB with exponential backoff retry.

        Args:
            alert: Document dictionary to insert.
            max_retries: Maximum insertion retries.
            backoff: Initial wait time multiplier.

        Returns:
            True if inserted successfully, False otherwise.
        """
        if not self._ensure_connected():
            logger.error("Cannot insert alert: No active connection to MongoDB.")
            return False

        for attempt in range(1, max_retries + 1):
            try:
                self._collection.insert_one(alert)
                return True
            except PyMongoError as e:
                logger.warning("MongoDB insert attempt %d/%d failed: %s", attempt, max_retries, e)
                if attempt < max_retries:
                    time.sleep(backoff * (2 ** (attempt - 1)))  # Exponential backoff
                else:
                    logger.error("❌ Failed to write fraud alert to MongoDB after %d attempts.", max_retries)
        return False

    def insert_alerts_batch(self, alerts: List[Dict[str, Any]], max_retries: int = 3, backoff: float = 1.0) -> int:
        """Insert multiple fraud alerts into MongoDB using bulk operations with retry.

        Args:
            alerts: List of alert dictionaries.
            max_retries: Maximum insertion retries.
            backoff: Initial wait time multiplier.

        Returns:
            Number of successfully inserted documents.
        """
        if not alerts:
            return 0
        if not self._ensure_connected():
            logger.error("Cannot insert alerts batch: No active connection to MongoDB.")
            return 0

        for attempt in range(1, max_retries + 1):
            try:
                result = self._collection.insert_many(alerts, ordered=False)
                inserted_count = len(result.inserted_ids)
                logger.info("Successfully persisted %d fraud alerts to MongoDB.", inserted_count)
                return inserted_count
            except PyMongoError as e:
                logger.warning("MongoDB batch insert attempt %d/%d failed: %s", attempt, max_retries, e)
                if attempt < max_retries:
                    time.sleep(backoff * (2 ** (attempt - 1)))
                else:
                    logger.error("❌ Failed to write batch alerts to MongoDB after %d attempts.", max_retries)
        return 0

    def close(self) -> None:
        """Close connection."""
        if self._client:
            self._client.close()
            logger.info("MongoDB client connection closed.")
            self._client = None
