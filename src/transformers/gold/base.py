"""
Base Gold transformer - provides SCD Type 2 and star schema patterns.
"""
import logging
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, md5, concat_ws,
    row_number, when, max as spark_max
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType

log = logging.getLogger(__name__)


class BaseGoldTransformer(ABC):
    """
    Abstract base class for Gold dimensional models.
    Supports both dimensions (with SCD Type 2) and fact tables.
    """

    def __init__(self, spark: SparkSession, table_name: str):
        """
        Initialize transformer.

        Args:
            spark: SparkSession
            table_name: Gold table name (e.g., 'DIM_EMPLOYEES')
        """
        self.spark = spark
        self.table_name = table_name

    @abstractmethod
    def get_schema(self) -> StructType:
        """Define Gold table schema."""
        pass

    @abstractmethod
    def transform(self, silver_df: DataFrame) -> DataFrame:
        """Transform Silver data to Gold."""
        pass

    def generate_surrogate_key(self, df: DataFrame, columns: list, key_name: str = "SK") -> DataFrame:
        """
        Generate surrogate key from business keys.

        Args:
            df: DataFrame
            columns: Columns to hash
            key_name: Name for surrogate key column

        Returns:
            DataFrame with surrogate key
        """
        return df.withColumn(
            key_name,
            md5(concat_ws("||", *[col(c) for c in columns]))
        )

    def apply_scd_type2(
        self,
        new_df: DataFrame,
        existing_df: DataFrame,
        business_keys: list,
        compare_columns: list,
        surrogate_key: str = "SK"
    ) -> DataFrame:
        """
        Apply SCD Type 2 logic for dimension tables.

        Args:
            new_df: New/updated records from Silver
            existing_df: Existing records from Gold
            business_keys: Natural keys (e.g., ['ID'])
            compare_columns: Columns to compare for changes
            surrogate_key: Surrogate key column name

        Returns:
            DataFrame with SCD Type 2 applied (inserts + updates)
        """
        log.info(f"[{self.table_name}] Applying SCD Type 2...")

        # If no existing data, treat all as new inserts
        if existing_df is None or existing_df.isEmpty():
            log.info(f"[{self.table_name}] No existing data, treating all as new records")
            return new_df.withColumn("VALID_FROM", current_timestamp()) \
                         .withColumn("VALID_TO", lit(None).cast("timestamp")) \
                         .withColumn("IS_CURRENT", lit(True))

        # Get current records only
        current_existing = existing_df.filter(col("IS_CURRENT") == True)

        # Create comparison hash for change detection
        hash_cols = compare_columns
        new_with_hash = new_df.withColumn(
            "_compare_hash",
            md5(concat_ws("||", *[col(c) for c in hash_cols]))
        )
        existing_with_hash = current_existing.withColumn(
            "_compare_hash",
            md5(concat_ws("||", *[col(c) for c in hash_cols]))
        )

        # Join on business keys
        join_condition = [new_with_hash[k] == existing_with_hash[k] for k in business_keys]
        joined = new_with_hash.alias("new").join(
            existing_with_hash.alias("existing"),
            join_condition,
            "left_outer"
        )

        # Classify records
        # 1. New records (no match in existing)
        new_records = joined.filter(col("existing._compare_hash").isNull()) \
            .select("new.*") \
            .withColumn("VALID_FROM", current_timestamp()) \
            .withColumn("VALID_TO", lit(None).cast("timestamp")) \
            .withColumn("IS_CURRENT", lit(True))

        # 2. Changed records (hash mismatch)
        changed_records = joined.filter(
            (col("existing._compare_hash").isNotNull()) &
            (col("new._compare_hash") != col("existing._compare_hash"))
        ).select("new.*") \
         .withColumn("VALID_FROM", current_timestamp()) \
         .withColumn("VALID_TO", lit(None).cast("timestamp")) \
         .withColumn("IS_CURRENT", lit(True))

        # 3. Unchanged records - keep as is
        unchanged_keys = joined.filter(
            (col("existing._compare_hash").isNotNull()) &
            (col("new._compare_hash") == col("existing._compare_hash"))
        ).select(*[col(f"new.{k}") for k in business_keys])

        # For changed records, we need to expire the old versions
        # This will be handled by MERGE SQL (setting IS_CURRENT=False, VALID_TO=NOW)

        # Combine new and changed records
        result = new_records.union(changed_records)

        log.info(f"[{self.table_name}] SCD Type 2: {new_records.count()} new, {changed_records.count()} changed")

        return result.drop("_compare_hash")

    def deduplicate(self, df: DataFrame, partition_by: list, order_by: str = "VALID_FROM") -> DataFrame:
        """
        Deduplicate DataFrame, keeping latest record per partition.

        Args:
            df: DataFrame
            partition_by: Columns to partition by
            order_by: Column to order by

        Returns:
            Deduplicated DataFrame
        """
        window_spec = Window.partitionBy(*partition_by).orderBy(col(order_by).desc())

        return df.withColumn("_row_num", row_number().over(window_spec)) \
                 .filter(col("_row_num") == 1) \
                 .drop("_row_num")

    def run(self, silver_df: DataFrame, existing_gold_df: DataFrame = None) -> DataFrame:
        """
        Execute full Gold transformation.

        Args:
            silver_df: Silver DataFrame
            existing_gold_df: Existing Gold data (for SCD Type 2)

        Returns:
            Gold DataFrame
        """
        log.info(f"[{self.table_name}] Starting Gold transformation...")

        # Apply transformation
        df = self.transform(silver_df)

        log.info(f"[SUCCESS] [{self.table_name}] Transformation complete ({df.count()} rows)")

        return df
