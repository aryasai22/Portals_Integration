"""
Silver transformer for CEIPAL expenses.
"""
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType
)
from pyspark.sql.functions import col, upper

from src.transformers.silver.base import BaseSilverTransformer


class ExpensesTransformer(BaseSilverTransformer):
    """Transform CEIPAL expenses to Silver layer."""

    def __init__(self, spark):
        super().__init__(spark, "expenses")

    def get_schema(self) -> StructType:
        return StructType([
            StructField("id", StringType(), False),
            StructField("expense_code", StringType(), True),
            StructField("employee_id", StringType(), True),
            StructField("employee_name", StringType(), True),
            StructField("project_id", StringType(), True),
            StructField("project_name", StringType(), True),
            StructField("expense_type", StringType(), True),
            StructField("category", StringType(), True),
            StructField("description", StringType(), True),
            StructField("expense_date", TimestampType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("status", StringType(), True),
            StructField("submitted_date", TimestampType(), True),
            StructField("approved_date", TimestampType(), True),
            StructField("approver_id", StringType(), True),
            StructField("approver_name", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
        ])

    def transform(self, df: DataFrame) -> DataFrame:
        # Clean string fields
        for col_name in ["expense_code", "employee_name", "project_name", "description", "category"]:
            if col_name in df.columns:
                df = self.clean_string(df, col_name)

        # Standardize status and type
        if "status" in df.columns:
            df = df.withColumn("status", upper(col("status")))
        if "expense_type" in df.columns:
            df = df.withColumn("expense_type", upper(col("expense_type")))

        # Parse dates
        for date_col in ["expense_date", "submitted_date", "approved_date", "created_at", "updated_at"]:
            if date_col in df.columns:
                df = self.standardize_date(df, date_col)

        # Add flags
        df = df.withColumn(
            "IS_APPROVED",
            col("status").isin(["APPROVED", "PAID"])
        ).withColumn(
            "IS_REIMBURSED",
            col("status") == "PAID"
        )

        return df
