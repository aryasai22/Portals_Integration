"""
Gold dimension: DIM_EMPLOYEES (SCD Type 2)
"""
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
    TimestampType, DoubleType, IntegerType
)
from pyspark.sql.functions import col, lit, monotonically_increasing_id

from src.transformers.gold.base import BaseGoldTransformer


class DimEmployeesTransformer(BaseGoldTransformer):
    """Transform employees to dimensional model with SCD Type 2."""

    def __init__(self, spark):
        super().__init__(spark, "DIM_EMPLOYEES")

    def get_schema(self) -> StructType:
        return StructType([
            # Surrogate key
            StructField("EMPLOYEE_SK", IntegerType(), False),

            # Natural key
            StructField("EMPLOYEE_ID", StringType(), False),

            # Attributes
            StructField("EMPLOYEE_CODE", StringType(), True),
            StructField("FIRST_NAME", StringType(), True),
            StructField("LAST_NAME", StringType(), True),
            StructField("FULL_NAME", StringType(), True),
            StructField("EMAIL", StringType(), True),
            StructField("PHONE", StringType(), True),
            StructField("JOB_TITLE", StringType(), True),
            StructField("DEPARTMENT", StringType(), True),
            StructField("STATUS", StringType(), True),
            StructField("IS_ACTIVE", BooleanType(), True),
            StructField("HIRE_DATE", TimestampType(), True),

            # SCD Type 2 fields
            StructField("VALID_FROM", TimestampType(), False),
            StructField("VALID_TO", TimestampType(), True),
            StructField("IS_CURRENT", BooleanType(), False),
        ])

    def transform(self, silver_df: DataFrame) -> DataFrame:
        """
        Transform Silver employees to Gold dimension.
        """
        gold_df = silver_df.select(
            col("ID").alias("EMPLOYEE_ID"),
            col("EMPLOYEE_CODE"),
            col("FIRST_NAME"),
            col("LAST_NAME"),
            col("FULL_NAME"),
            col("EMAIL"),
            col("PHONE"),
            col("JOB_TITLE"),
            col("DEPARTMENT"),
            col("STATUS"),
            col("IS_ACTIVE"),
            col("HIRE_DATE")
        )

        # Add surrogate key
        gold_df = gold_df.withColumn("EMPLOYEE_SK", monotonically_increasing_id())

        return gold_df
