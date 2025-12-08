"""
Gold dimension: DIM_CLIENTS (SCD Type 2)
"""
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
    TimestampType, IntegerType
)
from pyspark.sql.functions import col, monotonically_increasing_id

from src.transformers.gold.base import BaseGoldTransformer


class DimClientsTransformer(BaseGoldTransformer):
    """Transform clients to dimensional model with SCD Type 2."""

    def __init__(self, spark):
        super().__init__(spark, "DIM_CLIENTS")

    def get_schema(self) -> StructType:
        return StructType([
            StructField("CLIENT_SK", IntegerType(), False),
            StructField("CLIENT_ID", StringType(), False),
            StructField("CLIENT_CODE", StringType(), True),
            StructField("CLIENT_NAME", StringType(), True),
            StructField("INDUSTRY", StringType(), True),
            StructField("STATUS", StringType(), True),
            StructField("IS_ACTIVE", BooleanType(), True),
            StructField("CITY", StringType(), True),
            StructField("STATE", StringType(), True),
            StructField("COUNTRY", StringType(), True),
            StructField("VALID_FROM", TimestampType(), False),
            StructField("VALID_TO", TimestampType(), True),
            StructField("IS_CURRENT", BooleanType(), False),
        ])

    def transform(self, silver_df: DataFrame) -> DataFrame:
        gold_df = silver_df.select(
            col("ID").alias("CLIENT_ID"),
            col("CLIENT_CODE"),
            col("CLIENT_NAME"),
            col("INDUSTRY"),
            col("STATUS"),
            col("IS_ACTIVE"),
            col("CITY"),
            col("STATE"),
            col("COUNTRY")
        )

        gold_df = gold_df.withColumn("CLIENT_SK", monotonically_increasing_id())

        return gold_df
