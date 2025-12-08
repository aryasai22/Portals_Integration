"""
Gold fact table: FACT_PLACEMENTS
"""
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, TimestampType
)
from pyspark.sql.functions import col, to_date

from src.transformers.gold.base import BaseGoldTransformer


class FactPlacementsTransformer(BaseGoldTransformer):
    """Transform placements to fact table."""

    def __init__(self, spark):
        super().__init__(spark, "FACT_PLACEMENTS")

    def get_schema(self) -> StructType:
        return StructType([
            # Fact table surrogate key
            StructField("PLACEMENT_SK", IntegerType(), False),

            # Foreign keys to dimensions
            StructField("EMPLOYEE_SK", IntegerType(), True),
            StructField("PROJECT_SK", IntegerType(), True),
            StructField("CLIENT_SK", IntegerType(), True),
            StructField("START_DATE_SK", IntegerType(), True),
            StructField("END_DATE_SK", IntegerType(), True),

            # Degenerate dimensions
            StructField("PLACEMENT_ID", StringType(), False),
            StructField("PLACEMENT_CODE", StringType(), True),
            StructField("STATUS", StringType(), True),

            # Measures
            StructField("BILL_RATE", DoubleType(), True),
            StructField("PAY_RATE", DoubleType(), True),
            StructField("MARGIN", DoubleType(), True),
            StructField("MARGIN_PCT", DoubleType(), True),
            StructField("DURATION_DAYS", IntegerType(), True),

            # Dates
            StructField("START_DATE", DateType(), True),
            StructField("END_DATE", DateType(), True),

            # Metadata
            StructField("CREATED_AT", TimestampType(), True),
        ])

    def transform(self, silver_df: DataFrame) -> DataFrame:
        """
        Transform Silver placements to Gold fact table.
        Note: Surrogate key lookups will be done in the pipeline via joins.
        """
        fact_df = silver_df.select(
            col("ID").alias("PLACEMENT_ID"),
            col("PLACEMENT_CODE"),
            col("EMPLOYEE_ID"),
            col("PROJECT_ID"),
            col("CLIENT_ID"),
            col("STATUS"),
            col("BILL_RATE"),
            col("PAY_RATE"),
            col("MARGIN"),
            col("MARGIN_PCT"),
            col("DURATION_DAYS"),
            to_date(col("START_DATE")).alias("START_DATE"),
            to_date(col("END_DATE")).alias("END_DATE"),
            col("CREATED_AT")
        )

        # Generate date surrogate keys (YYYYMMDD format)
        fact_df = fact_df.withColumn(
            "START_DATE_SK",
            (col("START_DATE").substr(1, 4).cast("int") * 10000 +
             col("START_DATE").substr(6, 2).cast("int") * 100 +
             col("START_DATE").substr(9, 2).cast("int"))
        ).withColumn(
            "END_DATE_SK",
            (col("END_DATE").substr(1, 4).cast("int") * 10000 +
             col("END_DATE").substr(6, 2).cast("int") * 100 +
             col("END_DATE").substr(9, 2).cast("int"))
        )

        return fact_df
