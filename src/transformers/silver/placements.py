"""
Silver transformer for CEIPAL placements.
"""
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType
)
from pyspark.sql.functions import col, upper, datediff

from src.transformers.silver.base import BaseSilverTransformer


class PlacementsTransformer(BaseSilverTransformer):
    """Transform CEIPAL placements to Silver layer."""

    def __init__(self, spark):
        super().__init__(spark, "placements")

    def get_schema(self) -> StructType:
        return StructType([
            StructField("id", StringType(), False),
            StructField("placement_code", StringType(), True),
            StructField("employee_id", StringType(), True),
            StructField("employee_name", StringType(), True),
            StructField("project_id", StringType(), True),
            StructField("project_name", StringType(), True),
            StructField("client_id", StringType(), True),
            StructField("client_name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("start_date", TimestampType(), True),
            StructField("end_date", TimestampType(), True),
            StructField("bill_rate", DoubleType(), True),
            StructField("pay_rate", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("placement_type", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
        ])

    def transform(self, df: DataFrame) -> DataFrame:
        # Clean string fields
        for col_name in ["placement_code", "employee_name", "project_name", "client_name"]:
            if col_name in df.columns:
                df = self.clean_string(df, col_name)

        # Standardize status and type
        if "status" in df.columns:
            df = df.withColumn("status", upper(col("status")))
        if "placement_type" in df.columns:
            df = df.withColumn("placement_type", upper(col("placement_type")))

        # Parse dates
        for date_col in ["start_date", "end_date", "created_at", "updated_at"]:
            if date_col in df.columns:
                df = self.standardize_date(df, date_col)

        # Calculate margin (bill_rate - pay_rate)
        if "bill_rate" in df.columns and "pay_rate" in df.columns:
            df = df.withColumn(
                "MARGIN",
                col("bill_rate") - col("pay_rate")
            ).withColumn(
                "MARGIN_PCT",
                ((col("bill_rate") - col("pay_rate")) / col("bill_rate") * 100)
            )

        # Calculate duration in days
        if "start_date" in df.columns and "end_date" in df.columns:
            df = df.withColumn(
                "DURATION_DAYS",
                datediff(col("end_date"), col("start_date"))
            )

        # Add IS_ACTIVE flag
        df = df.withColumn(
            "IS_ACTIVE",
            col("status").isin(["ACTIVE", "ONGOING"])
        )

        return df
