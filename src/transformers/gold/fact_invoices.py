"""
Gold fact table: FACT_INVOICES
"""
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, TimestampType, BooleanType
)
from pyspark.sql.functions import col, to_date

from src.transformers.gold.base import BaseGoldTransformer


class FactInvoicesTransformer(BaseGoldTransformer):
    """Transform invoices to fact table."""

    def __init__(self, spark):
        super().__init__(spark, "FACT_INVOICES")

    def get_schema(self) -> StructType:
        return StructType([
            StructField("INVOICE_SK", IntegerType(), False),
            StructField("CLIENT_SK", IntegerType(), True),
            StructField("PROJECT_SK", IntegerType(), True),
            StructField("INVOICE_DATE_SK", IntegerType(), True),
            StructField("DUE_DATE_SK", IntegerType(), True),
            StructField("INVOICE_ID", StringType(), False),
            StructField("INVOICE_NUMBER", StringType(), True),
            StructField("STATUS", StringType(), True),
            StructField("SUBTOTAL", DoubleType(), True),
            StructField("TAX", DoubleType(), True),
            StructField("TOTAL_AMOUNT", DoubleType(), True),
            StructField("CURRENCY", StringType(), True),
            StructField("IS_PAID", BooleanType(), True),
            StructField("IS_OVERDUE", BooleanType(), True),
            StructField("INVOICE_DATE", DateType(), True),
            StructField("DUE_DATE", DateType(), True),
            StructField("PAYMENT_DATE", DateType(), True),
            StructField("CREATED_AT", TimestampType(), True),
        ])

    def transform(self, silver_df: DataFrame) -> DataFrame:
        fact_df = silver_df.select(
            col("ID").alias("INVOICE_ID"),
            col("INVOICE_NUMBER"),
            col("CLIENT_ID"),
            col("PROJECT_ID"),
            col("STATUS"),
            col("SUBTOTAL"),
            col("TAX"),
            col("TOTAL_AMOUNT"),
            col("CURRENCY"),
            col("IS_PAID"),
            col("IS_OVERDUE"),
            to_date(col("INVOICE_DATE")).alias("INVOICE_DATE"),
            to_date(col("DUE_DATE")).alias("DUE_DATE"),
            to_date(col("PAYMENT_DATE")).alias("PAYMENT_DATE"),
            col("CREATED_AT")
        )

        # Generate date surrogate keys
        fact_df = fact_df.withColumn(
            "INVOICE_DATE_SK",
            (col("INVOICE_DATE").substr(1, 4).cast("int") * 10000 +
             col("INVOICE_DATE").substr(6, 2).cast("int") * 100 +
             col("INVOICE_DATE").substr(9, 2).cast("int"))
        ).withColumn(
            "DUE_DATE_SK",
            (col("DUE_DATE").substr(1, 4).cast("int") * 10000 +
             col("DUE_DATE").substr(6, 2).cast("int") * 100 +
             col("DUE_DATE").substr(9, 2).cast("int"))
        )

        return fact_df
