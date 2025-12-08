"""Base class for Silver transformers with common cleaning logic"""
import logging
from abc import ABC, abstractmethod
from typing import List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_timestamp,
    when, trim, upper, lower, regexp_replace, coalesce
)
from pyspark.sql.types import StructType

log = logging.getLogger(__name__)


class BaseSilverTransformer(ABC):
    """Base transformer - implement get_schema() and transform() in subclasses"""

    def __init__(self, spark, resource_name):
        self.spark = spark
        self.resource_name = resource_name
        self.table_name = f"CEIPAL_{resource_name.upper()}"

    @abstractmethod
    def get_schema(self):
        pass

    @abstractmethod
    def transform(self, bronze_df):
        """
        Transform Bronze data to Silver.

        Args:
            bronze_df: Bronze DataFrame with columns (ID, DATA, RECORD_HASH, LOADED_AT)

        Returns:
            Silver DataFrame with typed, cleaned columns
        """
        pass

    def parse_json_data(self, bronze_df: DataFrame) -> DataFrame:
        """
        Parse JSON DATA column from Bronze into typed columns.

        Args:
            bronze_df: Bronze DataFrame

        Returns:
            DataFrame with parsed columns
        """
        log.info(f"[{self.resource_name}] Parsing JSON DATA column...")

        schema = self.get_schema()

        # Extract JSON fields
        from pyspark.sql.functions import from_json

        parsed_df = bronze_df.withColumn(
            "parsed_data",
            from_json(col("DATA"), schema)
        )

        # Flatten parsed data
        parsed_df = parsed_df.select(
            col("ID").alias("BRONZE_ID"),
            col("RECORD_HASH"),
            col("LOADED_AT").alias("BRONZE_LOADED_AT"),
            "parsed_data.*"
        )

        return parsed_df

    def clean_string(self, df: DataFrame, column: str) -> DataFrame:
        """
        Clean string column (trim, handle nulls).

        Args:
            df: DataFrame
            column: Column name

        Returns:
            DataFrame with cleaned column
        """
        return df.withColumn(
            column,
            when(
                trim(col(column)) == "",
                lit(None)
            ).otherwise(
                trim(col(column))
            )
        )

    def clean_email(self, df: DataFrame, column: str) -> DataFrame:
        """
        Clean and validate email column.

        Args:
            df: DataFrame
            column: Column name

        Returns:
            DataFrame with cleaned email
        """
        return df.withColumn(
            column,
            when(
                col(column).rlike(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
                lower(trim(col(column)))
            ).otherwise(
                lit(None)
            )
        )

    def clean_phone(self, df: DataFrame, column: str) -> DataFrame:
        """
        Clean phone number (remove formatting, keep digits only).

        Args:
            df: DataFrame
            column: Column name

        Returns:
            DataFrame with cleaned phone
        """
        return df.withColumn(
            column,
            regexp_replace(col(column), r'[^0-9]', '')
        ).withColumn(
            column,
            when(
                col(column) == "",
                lit(None)
            ).otherwise(
                col(column)
            )
        )

    def standardize_date(self, df: DataFrame, column: str, format: str = None) -> DataFrame:
        """
        Standardize date/timestamp column.

        Args:
            df: DataFrame
            column: Column name
            format: Date format (if not ISO8601)

        Returns:
            DataFrame with standardized timestamp
        """
        if format:
            return df.withColumn(
                column,
                to_timestamp(col(column), format)
            )
        else:
            # Try to parse as ISO8601
            return df.withColumn(
                column,
                to_timestamp(col(column))
            )

    def add_silver_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add Silver metadata columns.

        Args:
            df: DataFrame

        Returns:
            DataFrame with metadata columns
        """
        return df.withColumn(
            "SILVER_PROCESSED_AT",
            current_timestamp()
        ).withColumn(
            "SILVER_IS_CURRENT",
            lit(True)
        )

    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply resource-specific business rules.
        Override this method in subclasses for custom rules.

        Args:
            df: DataFrame

        Returns:
            DataFrame with business rules applied
        """
        return df

    def get_merge_keys(self) -> List[str]:
        """
        Get merge keys for MERGE operations.
        Override in subclasses if different from ['ID'].

        Returns:
            List of column names to use as merge keys
        """
        return ["ID"]

    def run(self, bronze_df: DataFrame) -> DataFrame:
        """
        Execute full transformation pipeline.

        Args:
            bronze_df: Bronze DataFrame

        Returns:
            Silver DataFrame ready for loading
        """
        log.info(f"[{self.resource_name}] Starting Silver transformation...")

        # Parse JSON
        df = self.parse_json_data(bronze_df)

        # Apply resource-specific transformation
        df = self.transform(df)

        # Apply business rules
        df = self.apply_business_rules(df)

        # Add metadata
        df = self.add_silver_metadata(df)

        log.info(f"[SUCCESS] [{self.resource_name}] Transformation complete ({df.count()} rows)")

        return df
