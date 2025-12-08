"""
Gold dimension: DIM_DATE (static date dimension)
"""
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DateType, BooleanType
)
from pyspark.sql.functions import (
    col, year, month, dayofmonth, dayofweek, dayofyear,
    weekofyear, quarter, date_format
)
from datetime import datetime, timedelta

from src.transformers.gold.base import BaseGoldTransformer


class DimDateTransformer(BaseGoldTransformer):
    """Generate date dimension table."""

    def __init__(self, spark):
        super().__init__(spark, "DIM_DATE")

    def get_schema(self) -> StructType:
        return StructType([
            StructField("DATE_SK", IntegerType(), False),
            StructField("DATE", DateType(), False),
            StructField("YEAR", IntegerType(), False),
            StructField("QUARTER", IntegerType(), False),
            StructField("MONTH", IntegerType(), False),
            StructField("MONTH_NAME", StringType(), False),
            StructField("DAY", IntegerType(), False),
            StructField("DAY_OF_WEEK", IntegerType(), False),
            StructField("DAY_NAME", StringType(), False),
            StructField("DAY_OF_YEAR", IntegerType(), False),
            StructField("WEEK_OF_YEAR", IntegerType(), False),
            StructField("IS_WEEKEND", BooleanType(), False),
        ])

    def transform(self, silver_df: DataFrame = None) -> DataFrame:
        """
        Generate date dimension (doesn't depend on Silver data).

        Args:
            silver_df: Ignored (date dimension is independent)

        Returns:
            Date dimension DataFrame
        """
        # Generate date range (5 years: 2020-2025)
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2025, 12, 31)

        dates = []
        current_date = start_date

        while current_date <= end_date:
            dates.append((current_date,))
            current_date += timedelta(days=1)

        # Create DataFrame
        date_df = self.spark.createDataFrame(dates, ["DATE"])

        # Add date attributes
        date_df = date_df.withColumn("DATE_SK",
            (year(col("DATE")) * 10000 +
             month(col("DATE")) * 100 +
             dayofmonth(col("DATE")))
        )

        date_df = date_df \
            .withColumn("YEAR", year(col("DATE"))) \
            .withColumn("QUARTER", quarter(col("DATE"))) \
            .withColumn("MONTH", month(col("DATE"))) \
            .withColumn("MONTH_NAME", date_format(col("DATE"), "MMMM")) \
            .withColumn("DAY", dayofmonth(col("DATE"))) \
            .withColumn("DAY_OF_WEEK", dayofweek(col("DATE"))) \
            .withColumn("DAY_NAME", date_format(col("DATE"), "EEEE")) \
            .withColumn("DAY_OF_YEAR", dayofyear(col("DATE"))) \
            .withColumn("WEEK_OF_YEAR", weekofyear(col("DATE"))) \
            .withColumn("IS_WEEKEND", col("DAY_OF_WEEK").isin([1, 7]))  # Sunday=1, Saturday=7

        return date_df
