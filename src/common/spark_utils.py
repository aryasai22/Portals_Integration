"""
PySpark utilities - session management and DataFrame helpers.
"""
import logging
from pyspark.sql import SparkSession
from src.common.config import (
    SPARK_DRIVER_MEMORY,
    SPARK_EXECUTOR_MEMORY,
    SPARK_SHUFFLE_PARTITIONS
)

log = logging.getLogger(__name__)

# Singleton Spark session
_spark_session = None


def get_spark_session(app_name: str = "PortalsIntegration") -> SparkSession:
    """
    Get or create Spark session (singleton pattern).

    Args:
        app_name: Application name for Spark UI

    Returns:
        SparkSession instance
    """
    global _spark_session

    if _spark_session is None:
        log.info(f"Creating new Spark session: {app_name}")

        builder = SparkSession.builder.appName(app_name)

        # Add Snowflake connector JARs
        builder = builder.config(
            "spark.jars.packages",
            "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3,"
            "net.snowflake:snowflake-jdbc:3.13.30"
        )

        # Memory settings
        builder = builder.config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        builder = builder.config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
        builder = builder.config("spark.driver.maxResultSize", "2g")

        # Performance tuning
        builder = builder.config("spark.sql.shuffle.partitions", str(SPARK_SHUFFLE_PARTITIONS))
        builder = builder.config("spark.sql.adaptive.enabled", "true")
        builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        builder = builder.config("spark.sql.sources.partitionOverwriteMode", "dynamic")

        # Create session
        _spark_session = builder.getOrCreate()
        _spark_session.sparkContext.setLogLevel("WARN")

        log.info(f"[SUCCESS] Spark session created (version: {_spark_session.version})")

    return _spark_session


def stop_spark_session():
    """Stop the Spark session."""
    global _spark_session

    if _spark_session is not None:
        log.info("Stopping Spark session...")
        _spark_session.stop()
        _spark_session = None
        log.info("[SUCCESS] Spark session stopped")
