"""
database/mongodb.py — MongoDB Database connection and alerts insertion wrapper (Day 20).

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


# MongoDB Configuration
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://mongodb:27017"
)

MONGO_DB = os.getenv(
    "MONGO_DB",
    "fraud_detection"
)

ALERTS_COLLECTION = os.getenv(
    "MONGO_ALERTS_COLLECTION",
    "fraud_alerts"
)


class MongoDBClient:
    """Thread-safe MongoDB client wrapper with retry mechanism."""

    def __init__(
        self,
        uri: str = MONGO_URI,
        db_name: str = MONGO_DB
    ) -> None:

        self._uri = uri
        self._db_name = db_name
        self._client = None
        self._db = None
        self._collection = None

        self._connect()


    def _connect(self) -> None:
        """Establish MongoDB connection."""

        try:
            logger.info(
                "Connecting to MongoDB at: %s",
                self._uri
            )

            self._client = MongoClient(
                self._uri,
                serverSelectionTimeoutMS=5000
            )

            self._client.admin.command("ping")

            self._db = self._client[self._db_name]

            self._collection = self._db[
                ALERTS_COLLECTION
            ]

            logger.info(
                "Successfully connected to MongoDB database: %s",
                self._db_name
            )

        except (ConnectionFailure, PyMongoError) as e:

            logger.error(
                "Failed to connect to MongoDB: %s",
                e
            )

            self._client = None
            self._db = None
            self._collection = None


    def _ensure_connected(self) -> bool:
        """Reconnect if connection lost."""

        if self._client is None or self._collection is None:
            self._connect()

        return self._client is not None


    def insert_alert(
        self,
        alert: Dict[str, Any],
        max_retries: int = 3,
        backoff: float = 1.0
    ) -> bool:
        """Insert single fraud alert."""

        if not self._ensure_connected():
            return False


        for attempt in range(1, max_retries + 1):

            try:

                self._collection.insert_one(alert)

                logger.info(
                    "Fraud alert inserted successfully"
                )

                return True


            except PyMongoError as e:

                logger.warning(
                    "Insert attempt %d/%d failed: %s",
                    attempt,
                    max_retries,
                    e
                )

                if attempt < max_retries:
                    time.sleep(
                        backoff * (2 ** (attempt - 1))
                    )

        return False


    def insert_alerts_batch(
        self,
        alerts: List[Dict[str, Any]],
        max_retries: int = 3,
        backoff: float = 1.0
    ) -> int:
        """Insert multiple fraud alerts."""

        if not alerts:
            return 0


        if not self._ensure_connected():
            return 0


        for attempt in range(1, max_retries + 1):

            try:

                result = self._collection.insert_many(
                    alerts,
                    ordered=False
                )

                return len(result.inserted_ids)


            except PyMongoError as e:

                logger.warning(
                    "Batch insert attempt %d/%d failed: %s",
                    attempt,
                    max_retries,
                    e
                )

                if attempt < max_retries:
                    time.sleep(
                        backoff * (2 ** (attempt - 1))
                    )

        return 0


    def close(self) -> None:
        """Close MongoDB connection."""

        if self._client:

            self._client.close()

            logger.info(
                "MongoDB client connection closed"
            )

            self._client = None