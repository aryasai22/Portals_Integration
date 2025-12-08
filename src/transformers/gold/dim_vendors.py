"""
Gold dimension: DIM_VENDORS (SCD Type 2)
"""
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
    TimestampType, IntegerType
)
from pyspark.sql.functions import col, monotonically_increasing_id

from src.transformers.gold.base import BaseGoldTransformer


class DimVendorsTransformer(BaseGoldTransformer):
    """Transform vendors to dimensional model with SCD Type 2."""

    def __init__(self, spark):
        super().__init__(spark, "DIM_VENDORS")

    def get_schema(self) -> StructType:
        return StructType([
            StructField("VENDOR_SK", IntegerType(), False),
            StructField("VENDOR_ID", StringType(), False),
            StructField("VENDOR_CODE", StringType(), True),
            StructField("VENDOR_NAME", StringType(), True),
            StructField("VENDOR_TYPE", StringType(), True),
            StructField("STATUS", StringType(), True),
            StructField("IS_ACTIVE", BooleanType(), True),
            StructField("VALID_FROM", TimestampType(), False),
            StructField("VALID_TO", TimestampType(), True),
            StructField("IS_CURRENT", BooleanType(), False),
        ])

    def transform(self, silver_df: DataFrame) -> DataFrame:
        gold_df = silver_df.select(
            col("ID").alias("VENDOR_ID"),
            col("VENDOR_CODE"),
            col("VENDOR_NAME"),
            col("VENDOR_TYPE"),
            col("STATUS"),
            col("IS_ACTIVE")
        )

        gold_df = gold_df.withColumn("VENDOR_SK", monotonically_increasing_id())

        return gold_df
