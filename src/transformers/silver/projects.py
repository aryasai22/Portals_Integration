"""
Silver transformer for CEIPAL projects.
"""
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType
)
from pyspark.sql.functions import col, upper

from src.transformers.silver.base import BaseSilverTransformer


class ProjectsTransformer(BaseSilverTransformer):
    """Transform CEIPAL projects to Silver layer."""

    def __init__(self, spark):
        super().__init__(spark, "projects")

    def get_schema(self) -> StructType:
        return StructType([
            StructField("id", StringType(), False),
            StructField("project_code", StringType(), True),
            StructField("project_name", StringType(), True),
            StructField("description", StringType(), True),
            StructField("status", StringType(), True),
            StructField("client_id", StringType(), True),
            StructField("client_name", StringType(), True),
            StructField("start_date", TimestampType(), True),
            StructField("end_date", TimestampType(), True),
            StructField("budget", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("project_manager_id", StringType(), True),
            StructField("project_manager_name", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
        ])

    def transform(self, df: DataFrame) -> DataFrame:
        # Clean string fields
        for col_name in ["project_code", "project_name", "description", "client_name"]:
            if col_name in df.columns:
                df = self.clean_string(df, col_name)

        # Standardize status
        if "status" in df.columns:
            df = df.withColumn("status", upper(col("status")))

        # Parse dates
        for date_col in ["start_date", "end_date", "created_at", "updated_at"]:
            if date_col in df.columns:
                df = self.standardize_date(df, date_col)

        # Add IS_ACTIVE flag
        df = df.withColumn(
            "IS_ACTIVE",
            col("status").isin(["ACTIVE", "IN_PROGRESS"])
        )

        return df
