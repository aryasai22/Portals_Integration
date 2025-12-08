"""
Gold fact table: FACT_EXPENSES
"""
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, TimestampType, BooleanType
)
from pyspark.sql.functions import col, to_date

from src.transformers.gold.base import BaseGoldTransformer


class FactExpensesTransformer(BaseGoldTransformer):
    """Transform expenses to fact table."""

    def __init__(self, spark):
        super().__init__(spark, "FACT_EXPENSES")

    def get_schema(self) -> StructType:
        return StructType([
            StructField("EXPENSE_SK", IntegerType(), False),
            StructField("EMPLOYEE_SK", IntegerType(), True),
            StructField("PROJECT_SK", IntegerType(), True),
            StructField("EXPENSE_DATE_SK", IntegerType(), True),
            StructField("EXPENSE_ID", StringType(), False),
            StructField("EXPENSE_CODE", StringType(), True),
            StructField("EXPENSE_TYPE", StringType(), True),
            StructField("CATEGORY", StringType(), True),
            StructField("STATUS", StringType(), True),
            StructField("AMOUNT", DoubleType(), True),
            StructField("CURRENCY", StringType(), True),
            StructField("IS_APPROVED", BooleanType(), True),
            StructField("IS_REIMBURSED", BooleanType(), True),
            StructField("EXPENSE_DATE", DateType(), True),
            StructField("CREATED_AT", TimestampType(), True),
        ])

    def transform(self, silver_df: DataFrame) -> DataFrame:
        fact_df = silver_df.select(
            col("ID").alias("EXPENSE_ID"),
            col("EXPENSE_CODE"),
            col("EMPLOYEE_ID"),
            col("PROJECT_ID"),
            col("EXPENSE_TYPE"),
            col("CATEGORY"),
            col("STATUS"),
            col("AMOUNT"),
            col("CURRENCY"),
            col("IS_APPROVED"),
            col("IS_REIMBURSED"),
            to_date(col("EXPENSE_DATE")).alias("EXPENSE_DATE"),
            col("CREATED_AT")
        )

        # Generate date surrogate key
        fact_df = fact_df.withColumn(
            "EXPENSE_DATE_SK",
            (col("EXPENSE_DATE").substr(1, 4).cast("int") * 10000 +
             col("EXPENSE_DATE").substr(6, 2).cast("int") * 100 +
             col("EXPENSE_DATE").substr(9, 2).cast("int"))
        )

        return fact_df
