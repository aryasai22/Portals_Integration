# dump_ceipal_employees_spark.py
"""
Enhanced CEIPAL data extraction with PySpark + Snowflake support.
Extends the original script with distributed processing and data warehouse integration.
"""
from __future__ import annotations

import os
import sys
import json
import time
import argparse
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime

# Import original script functions
from dump_ceipal_employees import (
    setup_logging, TOKEN,
    fetch_all_employees, fetch_all_projects, fetch_all_placements,
    fetch_all_clients, fetch_all_vendors, fetch_all_expenses,
    fetch_all_countries, fetch_all_states, fetch_all_invoices,
    enrich_employees_with_details, enrich_projects_with_details,
    enrich_placements_with_details, enrich_clients_with_details,
    enrich_vendors_with_details, enrich_expenses_with_details,
    enrich_invoices_with_details
)

from dotenv import load_dotenv

# PySpark imports (optional)
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import (
        col, lit, current_timestamp, to_timestamp,
        from_json, explode, when, coalesce
    )
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType,
        DoubleType, TimestampType, BooleanType, ArrayType, MapType
    )
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

log = logging.getLogger("ceipal.spark")

# ============== Snowflake Configuration ==============
load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"), override=True)

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "").strip()
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "").strip()
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "").strip()
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "SYSADMIN").strip()
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH").strip()
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "CEIPAL_DW").strip()
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "RAW").strip()

# Advanced options
SNOWFLAKE_STAGE_SCHEMA = os.getenv("SNOWFLAKE_STAGE_SCHEMA", "STAGING").strip()
SNOWFLAKE_WAREHOUSE_SCHEMA = os.getenv("SNOWFLAKE_WAREHOUSE_SCHEMA", "WAREHOUSE").strip()
LOAD_STRATEGY = os.getenv("LOAD_STRATEGY", "append").strip()  # append, overwrite, merge


# ============== Schema Definitions ==============

EMPLOYEE_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("employee_id", StringType(), True),
    StructField("employee_number", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("status", StringType(), True),
    StructField("department", StringType(), True),
    StructField("job_title", StringType(), True),
    StructField("hire_date", StringType(), True),
    StructField("termination_date", StringType(), True),
    StructField("manager_id", StringType(), True),
    StructField("location", StringType(), True),
    StructField("detail", StringType(), True),  # JSON string
    StructField("load_timestamp", TimestampType(), True),
    StructField("source_file", StringType(), True),
])

PROJECT_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("project_id", StringType(), True),
    StructField("project_code", StringType(), True),
    StructField("project_name", StringType(), True),
    StructField("client_id", StringType(), True),
    StructField("client_name", StringType(), True),
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("project_manager", StringType(), True),
    StructField("detail", StringType(), True),
    StructField("load_timestamp", TimestampType(), True),
])

PLACEMENT_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("placement_id", StringType(), True),
    StructField("job_code", StringType(), True),
    StructField("employee_id", StringType(), True),
    StructField("client_id", StringType(), True),
    StructField("project_id", StringType(), True),
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("billing_rate", DoubleType(), True),
    StructField("pay_rate", DoubleType(), True),
    StructField("detail", StringType(), True),
    StructField("load_timestamp", TimestampType(), True),
])

CLIENT_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("client_id", StringType(), True),
    StructField("client_name", StringType(), True),
    StructField("client_code", StringType(), True),
    StructField("status", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("contact_name", StringType(), True),
    StructField("contact_email", StringType(), True),
    StructField("contact_phone", StringType(), True),
    StructField("detail", StringType(), True),
    StructField("load_timestamp", TimestampType(), True),
])


# ============== Spark Session Builder ==============

def create_spark_session(app_name: str = "CEIPAL-ETL") -> SparkSession:
    """Create or get SparkSession with Snowflake connector."""
    if not PYSPARK_AVAILABLE:
        raise RuntimeError("PySpark not installed. Run: pip install pyspark")

    builder = SparkSession.builder.appName(app_name)

    # Add Snowflake connector JAR
    builder = builder.config(
        "spark.jars.packages",
        "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3,net.snowflake:snowflake-jdbc:3.13.30"
    )

    # Performance tuning
    builder = builder.config("spark.sql.shuffle.partitions", "200")
    builder = builder.config("spark.sql.adaptive.enabled", "true")
    builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")

    # Memory settings (adjust based on your cluster)
    builder = builder.config("spark.driver.memory", "4g")
    builder = builder.config("spark.executor.memory", "4g")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    log.info("[spark] Session created: %s", spark.version)
    return spark


def get_snowflake_options() -> Dict[str, str]:
    """Build Snowflake connection options."""
    if not all([SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD]):
        raise ValueError(
            "Missing Snowflake credentials. Set: SNOWFLAKE_ACCOUNT, "
            "SNOWFLAKE_USER, SNOWFLAKE_PASSWORD in .env"
        )

    return {
        "sfURL": f"{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com",
        "sfUser": SNOWFLAKE_USER,
        "sfPassword": SNOWFLAKE_PASSWORD,
        "sfDatabase": SNOWFLAKE_DATABASE,
        "sfSchema": SNOWFLAKE_SCHEMA,
        "sfWarehouse": SNOWFLAKE_WAREHOUSE,
        "sfRole": SNOWFLAKE_ROLE,
    }


# ============== Data Transformation ==============

def create_dataframe_from_dict(
    spark: SparkSession,
    data: List[Dict[str, Any]],
    entity_type: str
) -> DataFrame:
    """
    Convert API response (list of dicts) to Spark DataFrame.
    Flattens nested 'detail' field to JSON string for schema flexibility.
    """
    if not data:
        log.warning("[spark] Empty dataset for %s", entity_type)
        return spark.createDataFrame([], StructType([]))

    # Convert nested detail object to JSON string
    for row in data:
        if "detail" in row and isinstance(row["detail"], dict):
            row["detail"] = json.dumps(row["detail"], ensure_ascii=False)
        elif "detail" in row and row["detail"] is None:
            row["detail"] = None

    # Create DataFrame with schema inference
    df = spark.createDataFrame(data)

    # Add metadata columns
    df = df.withColumn("load_timestamp", current_timestamp()) \
           .withColumn("source_system", lit("CEIPAL_API")) \
           .withColumn("load_date", lit(datetime.now().strftime("%Y-%m-%d")))

    log.info("[spark] Created DataFrame for %s: %d rows, %d columns",
             entity_type, df.count(), len(df.columns))

    return df


def flatten_detail_column(df: DataFrame, detail_fields: List[str]) -> DataFrame:
    """
    Optional: Flatten specific fields from JSON 'detail' column.
    Example: Extract detail.full_address, detail.tax_id, etc.
    """
    if "detail" not in df.columns:
        return df

    # Define JSON schema for parsing (customize per entity)
    detail_schema = StructType([
        StructField(field, StringType(), True) for field in detail_fields
    ])

    df_flattened = df.withColumn(
        "detail_parsed",
        from_json(col("detail"), detail_schema)
    )

    # Extract fields
    for field in detail_fields:
        df_flattened = df_flattened.withColumn(
            f"detail_{field}",
            col(f"detail_parsed.{field}")
        )

    return df_flattened.drop("detail_parsed")


# ============== Snowflake Loaders ==============

def write_to_snowflake(
    df: DataFrame,
    table_name: str,
    mode: str = "append",
    use_staging: bool = False
) -> None:
    """
    Write DataFrame to Snowflake table.

    Args:
        df: Spark DataFrame
        table_name: Target table name (without schema)
        mode: append, overwrite, or merge
        use_staging: Write to staging schema first
    """
    sf_options = get_snowflake_options()

    if use_staging:
        sf_options["sfSchema"] = SNOWFLAKE_STAGE_SCHEMA
        target_table = f"STG_{table_name.upper()}"
    else:
        target_table = table_name.upper()

    log.info("[snowflake] Writing to %s.%s.%s (mode=%s)",
             SNOWFLAKE_DATABASE, sf_options["sfSchema"], target_table, mode)

    try:
        df.write \
            .format("snowflake") \
            .options(**sf_options) \
            .option("dbtable", target_table) \
            .mode(mode) \
            .save()

        log.info("[snowflake] Successfully wrote %d rows to %s", df.count(), target_table)

    except Exception as e:
        log.error("[snowflake] Write failed: %s", str(e))
        raise


def execute_merge_statement(
    spark: SparkSession,
    staging_table: str,
    target_table: str,
    merge_keys: List[str],
    update_condition: Optional[str] = None
) -> None:
    """
    Execute MERGE (upsert) from staging to target table.
    Implements SCD Type 1 (update in place) by default.

    Args:
        staging_table: Source table in staging schema
        target_table: Target table in warehouse schema
        merge_keys: List of columns to match on (e.g., ['id', 'employee_id'])
        update_condition: Optional WHERE clause for updates (e.g., "src.modified_date > dst.modified_date")
    """
    sf_options = get_snowflake_options()

    # Build match condition
    match_conditions = " AND ".join([f"dst.{key} = src.{key}" for key in merge_keys])

    # Build update SET clause (all columns except keys and metadata)
    # Note: In production, explicitly list columns
    update_clause = "SET dst.* = src.*"  # Simplified; customize per table

    merge_sql = f"""
    MERGE INTO {SNOWFLAKE_WAREHOUSE_SCHEMA}.{target_table} AS dst
    USING {SNOWFLAKE_STAGE_SCHEMA}.{staging_table} AS src
    ON {match_conditions}
    WHEN MATCHED {"AND " + update_condition if update_condition else ""} THEN
        UPDATE {update_clause}
    WHEN NOT MATCHED THEN
        INSERT *
    """

    log.info("[snowflake] Executing MERGE into %s", target_table)
    log.debug("[snowflake] SQL: %s", merge_sql)

    try:
        # Use Snowflake Utils to execute SQL
        spark._jvm.net.snowflake.spark.snowflake.Utils.runQuery(
            sf_options,
            merge_sql
        )
        log.info("[snowflake] MERGE completed successfully")

    except Exception as e:
        log.error("[snowflake] MERGE failed: %s", str(e))
        raise


def load_with_merge_strategy(
    df: DataFrame,
    table_name: str,
    merge_keys: List[str]
) -> None:
    """
    Two-step load: staging → warehouse with MERGE.
    """
    staging_table = f"STG_{table_name.upper()}"
    target_table = table_name.upper()

    # Step 1: Write to staging (truncate-load)
    write_to_snowflake(df, table_name, mode="overwrite", use_staging=True)

    # Step 2: Merge into warehouse
    execute_merge_statement(
        df.sparkSession,
        staging_table,
        target_table,
        merge_keys
    )


# ============== ETL Orchestration ==============

def etl_employees(
    spark: SparkSession,
    args: argparse.Namespace
) -> None:
    """Extract-Transform-Load for Employees."""
    log.info("[etl] Starting EMPLOYEES extraction")

    # Extract
    rows = fetch_all_employees(
        args.limit, args.sleep, args.max_pages,
        args.connect_timeout, args.read_timeout, args.retries
    )

    if args.with_details and rows:
        log.info("[etl] Enriching with employee details")
        rows = enrich_employees_with_details(
            rows, args.workers, args.rps,
            args.connect_timeout, args.read_timeout, args.retries, args.log_every
        )

    # Transform
    df = create_dataframe_from_dict(spark, rows, "employees")

    if df.isEmpty():
        log.warning("[etl] No employees to load")
        return

    # Optional: Flatten detail fields
    # df = flatten_detail_column(df, ["full_address", "ssn", "tax_id"])

    # Load
    if LOAD_STRATEGY == "merge":
        load_with_merge_strategy(df, "employees", merge_keys=["id"])
    else:
        write_to_snowflake(df, "employees", mode=LOAD_STRATEGY)

    log.info("[etl] EMPLOYEES completed: %d rows loaded", df.count())


def etl_projects(
    spark: SparkSession,
    args: argparse.Namespace
) -> None:
    """ETL for Projects."""
    log.info("[etl] Starting PROJECTS extraction")

    rows = fetch_all_projects(
        args.limit, args.sleep, args.max_pages,
        args.connect_timeout, args.read_timeout, args.retries, args.offset
    )

    if args.with_details and rows:
        rows = enrich_projects_with_details(
            rows, args.workers, args.rps,
            args.connect_timeout, args.read_timeout, args.retries, args.log_every
        )

    df = create_dataframe_from_dict(spark, rows, "projects")

    if df.isEmpty():
        log.warning("[etl] No projects to load")
        return

    if LOAD_STRATEGY == "merge":
        load_with_merge_strategy(df, "projects", merge_keys=["id"])
    else:
        write_to_snowflake(df, "projects", mode=LOAD_STRATEGY)

    log.info("[etl] PROJECTS completed: %d rows", df.count())


def etl_placements(spark: SparkSession, args: argparse.Namespace) -> None:
    log.info("[etl] Starting PLACEMENTS extraction")
    rows = fetch_all_placements(args.limit, args.sleep, args.max_pages, args.connect_timeout, args.read_timeout, args.retries)
    if args.with_details and rows:
        rows = enrich_placements_with_details(rows, args.workers, args.rps, args.connect_timeout, args.read_timeout, args.retries, args.log_every)
    df = create_dataframe_from_dict(spark, rows, "placements")
    if not df.isEmpty():
        write_to_snowflake(df, "placements", mode=LOAD_STRATEGY)
        log.info("[etl] PLACEMENTS completed: %d rows", df.count())


def etl_clients(spark: SparkSession, args: argparse.Namespace) -> None:
    log.info("[etl] Starting CLIENTS extraction")
    rows = fetch_all_clients(args.limit, args.sleep, args.max_pages, args.connect_timeout, args.read_timeout, args.retries)
    if args.with_details and rows:
        rows = enrich_clients_with_details(rows, args.workers, args.rps, args.connect_timeout, args.read_timeout, args.retries, args.log_every)
    df = create_dataframe_from_dict(spark, rows, "clients")
    if not df.isEmpty():
        write_to_snowflake(df, "clients", mode=LOAD_STRATEGY)
        log.info("[etl] CLIENTS completed: %d rows", df.count())


def etl_vendors(spark: SparkSession, args: argparse.Namespace) -> None:
    log.info("[etl] Starting VENDORS extraction")
    rows = fetch_all_vendors(args.limit, args.sleep, args.max_pages, args.connect_timeout, args.read_timeout, args.retries)
    if args.with_details and rows:
        rows = enrich_vendors_with_details(rows, args.workers, args.rps, args.connect_timeout, args.read_timeout, args.retries, args.log_every)
    df = create_dataframe_from_dict(spark, rows, "vendors")
    if not df.isEmpty():
        write_to_snowflake(df, "vendors", mode=LOAD_STRATEGY)
        log.info("[etl] VENDORS completed: %d rows", df.count())


def etl_expenses(spark: SparkSession, args: argparse.Namespace) -> None:
    log.info("[etl] Starting EXPENSES extraction")
    rows = fetch_all_expenses(
        args.limit, args.sleep, args.max_pages, args.connect_timeout, args.read_timeout, args.retries,
        args.placement_id, args.employee_id, args.expense_status, args.from_date, args.to_date
    )
    if args.with_details and rows:
        rows = enrich_expenses_with_details(rows, args.workers, args.rps, args.connect_timeout, args.read_timeout, args.retries, args.log_every)
    df = create_dataframe_from_dict(spark, rows, "expenses")
    if not df.isEmpty():
        write_to_snowflake(df, "expenses", mode=LOAD_STRATEGY)
        log.info("[etl] EXPENSES completed: %d rows", df.count())


def etl_invoices(spark: SparkSession, args: argparse.Namespace) -> None:
    log.info("[etl] Starting INVOICES extraction")
    rows = fetch_all_invoices(
        args.limit, args.sleep, args.max_pages, args.connect_timeout, args.read_timeout, args.retries,
        args.from_date, args.to_date, args.client_name, args.invoice_status
    )
    if args.with_details and rows:
        rows = enrich_invoices_with_details(rows, args.workers, args.rps, args.connect_timeout, args.read_timeout, args.retries, args.log_every)
    df = create_dataframe_from_dict(spark, rows, "invoices")
    if not df.isEmpty():
        write_to_snowflake(df, "invoices", mode=LOAD_STRATEGY)
        log.info("[etl] INVOICES completed: %d rows", df.count())


# ============== Main ==============

def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="CEIPAL ETL with PySpark + Snowflake support"
    )

    # Entity selection
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--employees", action="store_true", help="Load Employees (default)")
    mode.add_argument("--projects", action="store_true")
    mode.add_argument("--placements", action="store_true")
    mode.add_argument("--clients", action="store_true")
    mode.add_argument("--vendors", action="store_true")
    mode.add_argument("--expenses", action="store_true")
    mode.add_argument("--invoices", action="store_true")
    mode.add_argument("--all", action="store_true", help="Load all entities")

    # API parameters
    p.add_argument("--limit", type=int, default=int(os.getenv("CEIPAL_LIMIT", "100")))
    p.add_argument("--sleep", type=float, default=0.2)
    p.add_argument("--max-pages", type=int, default=None)
    p.add_argument("--with-details", action="store_true")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--rps", type=float, default=2.0)

    # Snowflake/Spark options
    p.add_argument("--no-spark", action="store_true", help="Disable Spark, fallback to JSON files")
    p.add_argument("--load-strategy", choices=["append", "overwrite", "merge"],
                   default=LOAD_STRATEGY, help="Snowflake load strategy")
    p.add_argument("--partition-by", default=None, help="Partition DataFrame by column (e.g., load_date)")

    # Filters
    p.add_argument("--placement-id", default=None)
    p.add_argument("--employee-id", default=None)
    p.add_argument("--expense-status", type=int, default=None)
    p.add_argument("--from-date", default=None)
    p.add_argument("--to-date", default=None)
    p.add_argument("--client-name", default=None)
    p.add_argument("--invoice-status", default=None)
    p.add_argument("--country", default=None)

    # General
    p.add_argument("--connect-timeout", type=float, default=60.0)
    p.add_argument("--read-timeout", type=float, default=120.0)
    p.add_argument("--retries", type=int, default=6)
    p.add_argument("--offset", type=int, default=0)
    p.add_argument("--log-every", type=int, default=50)
    p.add_argument("--debug", action="store_true")
    p.add_argument("--http-debug", action="store_true")

    args = p.parse_args(argv)
    setup_logging(args.debug, args.http_debug)

    # Override global load strategy
    global LOAD_STRATEGY
    LOAD_STRATEGY = args.load_strategy

    # Check if Spark should be used
    if args.no_spark:
        log.warning("[main] --no-spark set; falling back to original JSON dump")
        # Import and run original main
        from dump_ceipal_employees import main as original_main
        return original_main(argv)

    if not PYSPARK_AVAILABLE:
        log.error("[main] PySpark not available. Install with: pip install pyspark")
        return 1

    # Create Spark session
    try:
        spark = create_spark_session()
    except Exception as e:
        log.error("[main] Failed to create Spark session: %s", e)
        return 1

    try:
        # Run selected ETL
        if args.all:
            log.info("[main] Running ETL for ALL entities")
            etl_employees(spark, args)
            etl_projects(spark, args)
            etl_placements(spark, args)
            etl_clients(spark, args)
            etl_vendors(spark, args)
            etl_expenses(spark, args)
            etl_invoices(spark, args)

        elif args.projects:
            etl_projects(spark, args)
        elif args.placements:
            etl_placements(spark, args)
        elif args.clients:
            etl_clients(spark, args)
        elif args.vendors:
            etl_vendors(spark, args)
        elif args.expenses:
            etl_expenses(spark, args)
        elif args.invoices:
            etl_invoices(spark, args)
        else:
            # Default: employees
            etl_employees(spark, args)

        log.info("[main] ETL completed successfully")
        return 0

    except Exception as e:
        log.exception("[main] ETL failed: %s", e)
        return 1

    finally:
        spark.stop()
        log.info("[main] Spark session stopped")


if __name__ == "__main__":
    sys.exit(main())
