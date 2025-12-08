"""
Snowflake utilities - connection management and write operations.
"""
import logging
from typing import Dict, List
from pyspark.sql import SparkSession, DataFrame

from src.common.config import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_ROLE,
    validate_snowflake_config
)

log = logging.getLogger(__name__)


def get_snowflake_options(schema: str = "RAW") -> Dict[str, str]:
    """
    Build Snowflake connection options.

    Args:
        schema: Target schema (RAW, STAGING, SILVER, GOLD)

    Returns:
        Dictionary of Snowflake connection options
    """
    validate_snowflake_config()

    return {
        "sfURL": f"{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com",
        "sfUser": SNOWFLAKE_USER,
        "sfPassword": SNOWFLAKE_PASSWORD,
        "sfDatabase": SNOWFLAKE_DATABASE,
        "sfSchema": schema,
        "sfWarehouse": SNOWFLAKE_WAREHOUSE,
        "sfRole": SNOWFLAKE_ROLE,
    }


def write_to_snowflake(
    df: DataFrame,
    table: str,
    schema: str = "RAW",
    mode: str = "append"
) -> None:
    """
    Write DataFrame to Snowflake table.

    Args:
        df: Spark DataFrame to write
        table: Target table name (will be uppercased)
        schema: Target schema (RAW, STAGING, SILVER, GOLD)
        mode: Write mode (append, overwrite, error)
    """
    if df.isEmpty():
        log.warning(f"[{schema}.{table}] Skipping write - DataFrame is empty")
        return

    table_upper = table.upper()
    sf_options = get_snowflake_options(schema)

    log.info(
        f"[{schema}.{table_upper}] Writing {df.count()} rows (mode={mode})"
    )

    try:
        df.write \
            .format("snowflake") \
            .options(**sf_options) \
            .option("dbtable", table_upper) \
            .mode(mode) \
            .save()

        log.info(f"[SUCCESS] [{schema}.{table_upper}] Write complete")

    except Exception as e:
        log.error(f"[ERROR] [{schema}.{table_upper}] Write failed: {e}")
        raise


def merge_to_snowflake(
    spark: SparkSession,
    df: DataFrame,
    table: str,
    merge_keys: List[str],
    target_schema: str = "SILVER",
    staging_schema: str = "STAGING"
) -> None:
    """
    Merge DataFrame to Snowflake using STAGING → MERGE pattern.

    Args:
        spark: SparkSession
        df: DataFrame to merge
        table: Target table name
        merge_keys: Columns to match on (e.g., ['id'])
        target_schema: Target schema (default: SILVER)
        staging_schema: Staging schema (default: STAGING)
    """
    if df.isEmpty():
        log.warning(f"[{target_schema}.{table}] Skipping merge - DataFrame is empty")
        return

    table_upper = table.upper()
    staging_table = f"STG_{table_upper}"

    # Phase 1: Write to STAGING
    log.info(f"[MERGE] Phase 1: Writing to {staging_schema}.{staging_table}")
    write_to_snowflake(df, staging_table, schema=staging_schema, mode="overwrite")

    # Phase 2: MERGE to target
    log.info(f"[MERGE] Phase 2: Merging {staging_schema}.{staging_table} → {target_schema}.{table_upper}")

    # Build MERGE SQL
    match_conditions = " AND ".join([f"tgt.{key} = src.{key}" for key in merge_keys])
    cols = [c for c in df.columns if not c.startswith("_")]
    update_set = ", ".join([f"tgt.{c} = src.{c}" for c in cols])
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join([f"src.{c}" for c in cols])

    merge_sql = f"""
    MERGE INTO {target_schema}.{table_upper} AS tgt
    USING {staging_schema}.{staging_table} AS src
    ON {match_conditions}
    WHEN MATCHED THEN
        UPDATE SET {update_set}
    WHEN NOT MATCHED THEN
        INSERT ({insert_cols})
        VALUES ({insert_vals})
    """

    log.debug(f"[MERGE] SQL: {merge_sql}")

    try:
        # Execute MERGE via Snowflake connector
        from py4j.java_gateway import java_import
        java_import(spark._jvm, "net.snowflake.spark.snowflake.Utils")

        sf_options = get_snowflake_options(target_schema)

        spark._jvm.net.snowflake.spark.snowflake.Utils.runQuery(
            sf_options,
            merge_sql
        )

        log.info(f"[SUCCESS] [MERGE] Complete: {target_schema}.{table_upper}")

    except Exception as e:
        log.error(f"[ERROR] [MERGE] Failed: {e}")
        raise


def read_from_snowflake(
    spark: SparkSession,
    table: str,
    schema: str = "RAW",
    filter_clause: str = None
) -> DataFrame:
    """
    Read DataFrame from Snowflake table.

    Args:
        spark: SparkSession
        table: Table name
        schema: Schema name
        filter_clause: Optional SQL WHERE clause

    Returns:
        Spark DataFrame
    """
    sf_options = get_snowflake_options(schema)
    table_upper = table.upper()

    log.info(f"[{schema}.{table_upper}] Reading from Snowflake...")

    query = f"SELECT * FROM {schema}.{table_upper}"
    if filter_clause:
        query += f" WHERE {filter_clause}"

    df = spark.read \
        .format("snowflake") \
        .options(**sf_options) \
        .option("query", query) \
        .load()

    row_count = df.count()
    log.info(f"[SUCCESS] [{schema}.{table_upper}] Read {row_count} rows")

    return df
