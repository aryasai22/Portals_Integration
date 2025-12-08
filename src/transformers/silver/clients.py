"""
Silver transformer for CEIPAL clients.
"""
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)
from pyspark.sql.functions import col, upper

from src.transformers.silver.base import BaseSilverTransformer


class ClientsTransformer(BaseSilverTransformer):
    """Transform CEIPAL clients to Silver layer."""

    def __init__(self, spark):
        super().__init__(spark, "clients")

    def get_schema(self) -> StructType:
        return StructType([
            StructField("id", StringType(), False),
            StructField("client_code", StringType(), True),
            StructField("client_name", StringType(), True),
            StructField("industry", StringType(), True),
            StructField("status", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("email", StringType(), True),
            StructField("website", StringType(), True),
            StructField("account_manager_id", StringType(), True),
            StructField("account_manager_name", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
        ])

    def transform(self, df: DataFrame) -> DataFrame:
        # Clean string fields
        for col_name in ["client_code", "client_name", "industry", "city", "state", "country"]:
            if col_name in df.columns:
                df = self.clean_string(df, col_name)

        # Standardize status
        if "status" in df.columns:
            df = df.withColumn("status", upper(col("status")))

        # Clean contact fields
        if "email" in df.columns:
            df = self.clean_email(df, "email")
        if "phone" in df.columns:
            df = self.clean_phone(df, "phone")

        # Parse dates
        for date_col in ["created_at", "updated_at"]:
            if date_col in df.columns:
                df = self.standardize_date(df, date_col)

        # Add IS_ACTIVE flag
        df = df.withColumn(
            "IS_ACTIVE",
            col("status").isin(["ACTIVE"])
        )

        return df
