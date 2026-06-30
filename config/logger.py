import logging
import os

# Create logs folder
os.makedirs("logs", exist_ok=True)

logger = logging.getLogger("fraud_detection")
logger.setLevel(logging.INFO)

if not logger.handlers:
    file_handler = logging.FileHandler("logs/application.log")
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)