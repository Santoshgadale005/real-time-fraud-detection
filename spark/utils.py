import os
import logging
from pathlib import Path
from pyspark.sql import SparkSession

logger = logging.getLogger("spark-utils")

# Solve compatibility issues with Java 24 by using Java 21 if installed
jdk_21_path = Path("/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home")
if jdk_21_path.exists():
    os.environ["JAVA_HOME"] = str(jdk_21_path)


def get_spark_session(app_name: str = "FraudDetection") -> SparkSession:
    """Create and configure a SparkSession with Kafka streaming support."""
    logger.info("Initializing Spark Session with name: %s", app_name)
    
    # Use Spark SQL Kafka package (Scala 2.13 version is appropriate for Spark 3.5.0+ and 4.x)
    # We load both spark-sql-kafka and its dependencies
    spark_jars = "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0"
    
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", spark_jars) \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g")
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("Spark Session successfully created (Spark version: %s)", spark.version)
    return spark
