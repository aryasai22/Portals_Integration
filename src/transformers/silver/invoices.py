"""
Silver transformer for CEIPAL invoices.
"""
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType
)
from pyspark.sql.functions import col, upper

from src.transformers.silver.base import BaseSilverTransformer


class InvoicesTransformer(BaseSilverTransformer):
    """Transform CEIPAL invoices to Silver layer."""

    def __init__(self, spark):
        super().__init__(spark, "invoices")

    def get_schema(self) -> StructType:
        return StructType([
            StructField("id", StringType(), False),
            StructField("invoice_number", StringType(), True),
            StructField("client_id", StringType(), True),
            StructField("client_name", StringType(), True),
            StructField("project_id", StringType(), True),
            StructField("project_name", StringType(), True),
            StructField("placement_id", StringType(), True),
            StructField("invoice_date", TimestampType(), True),
            StructField("due_date", TimestampType(), True),
            StructField("period_start", TimestampType(), True),
            StructField("period_end", TimestampType(), True),
            StructField("subtotal", DoubleType(), True),
            StructField("tax", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("status", StringType(), True),
            StructField("payment_date", TimestampType(), True),
            StructField("payment_method", StringType(), True),
            StructField("notes", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
        ])

    def transform(self, df: DataFrame) -> DataFrame:
        # Clean string fields
        for col_name in ["invoice_number", "client_name", "project_name", "notes"]:
            if col_name in df.columns:
                df = self.clean_string(df, col_name)

        # Standardize status and payment method
        if "status" in df.columns:
            df = df.withColumn("status", upper(col("status")))
        if "payment_method" in df.columns:
            df = df.withColumn("payment_method", upper(col("payment_method")))

        # Parse dates
        for date_col in ["invoice_date", "due_date", "period_start", "period_end", "payment_date", "created_at", "updated_at"]:
            if date_col in df.columns:
                df = self.standardize_date(df, date_col)

        # Add flags
        df = df.withColumn(
            "IS_PAID",
            col("status").isin(["PAID", "CLOSED"])
        ).withColumn(
            "IS_OVERDUE",
            (col("status").isin(["PENDING", "SENT", "UNPAID"])) &
            (col("due_date") < col("current_timestamp"))
        )

        return df
