"""
Gold Modeling Pipeline - Build star schema from Silver layer.

Usage:
    python pipelines/gold_modeling.py --all
    python pipelines/gold_modeling.py --dimensions
    python pipelines/gold_modeling.py --facts
    python pipelines/gold_modeling.py --rebuild
"""
import argparse
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.common.config import validate_snowflake_config, GOLD_REBUILD_DIMS, GOLD_REBUILD_FACTS
from src.common.logging_utils import setup_logging, PipelineMetrics, log_section
from src.common.spark_utils import get_spark_session, stop_spark_session
from src.common.snowflake_utils import read_from_snowflake, write_to_snowflake

# Import Gold transformers
from src.transformers.gold.dim_employees import DimEmployeesTransformer
from src.transformers.gold.dim_projects import DimProjectsTransformer
from src.transformers.gold.dim_clients import DimClientsTransformer
from src.transformers.gold.dim_vendors import DimVendorsTransformer
from src.transformers.gold.dim_date import DimDateTransformer
from src.transformers.gold.fact_placements import FactPlacementsTransformer
from src.transformers.gold.fact_expenses import FactExpensesTransformer
from src.transformers.gold.fact_invoices import FactInvoicesTransformer


DIMENSIONS = {
    "employees": ("CEIPAL_EMPLOYEES", DimEmployeesTransformer),
    "projects": ("CEIPAL_PROJECTS", DimProjectsTransformer),
    "clients": ("CEIPAL_CLIENTS", DimClientsTransformer),
    "vendors": ("CEIPAL_VENDORS", DimVendorsTransformer),
    "date": (None, DimDateTransformer),  # Date dimension doesn't read from Silver
}

FACTS = {
    "placements": ("CEIPAL_PLACEMENTS", FactPlacementsTransformer),
    "expenses": ("CEIPAL_EXPENSES", FactExpensesTransformer),
    "invoices": ("CEIPAL_INVOICES", FactInvoicesTransformer),
}


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="CEIPAL Gold Modeling Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # Model selection
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--all", action="store_true", help="Build all dimensions and facts")
    group.add_argument("--dimensions", action="store_true", help="Build dimensions only")
    group.add_argument("--facts", action="store_true", help="Build facts only")

    # Build options
    parser.add_argument("--rebuild", action="store_true", help="Full rebuild (overwrite mode)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    return parser.parse_args()


def build_dimension(spark, dim_name: str, silver_table: str, transformer_class, rebuild: bool, log):
    """Build a single dimension table."""
    log.info(f"[{dim_name}] Building dimension...")

    try:
        # Read from Silver (or generate for date dimension)
        if silver_table:
            log.info(f"[{dim_name}] Reading from SILVER.{silver_table}...")
            silver_df = read_from_snowflake(spark, silver_table, schema="SILVER")
        else:
            log.info(f"[{dim_name}] Generating dimension data...")
            silver_df = None

        # Transform
        transformer = transformer_class(spark)
        gold_df = transformer.run(silver_df)

        # Write to Gold
        gold_table = transformer.table_name
        mode = "overwrite" if rebuild else "append"

        log.info(f"[{dim_name}] Writing to GOLD.{gold_table} (mode={mode})...")
        write_to_snowflake(gold_df, gold_table, schema="GOLD", mode=mode)

        log.info(f"[SUCCESS] [{dim_name}] Dimension build complete ({gold_df.count()} rows)")
        return gold_df.count(), "SUCCESS"

    except Exception as e:
        log.error(f"[ERROR] [{dim_name}] Dimension build failed: {e}", exc_info=True)
        return 0, "FAILED"


def build_fact(spark, fact_name: str, silver_table: str, transformer_class, rebuild: bool, log):
    """Build a single fact table."""
    log.info(f"[{fact_name}] Building fact table...")

    try:
        # Read from Silver
        log.info(f"[{fact_name}] Reading from SILVER.{silver_table}...")
        silver_df = read_from_snowflake(spark, silver_table, schema="SILVER")

        # Transform
        transformer = transformer_class(spark)
        gold_df = transformer.run(silver_df)

        # Write to Gold
        gold_table = transformer.table_name
        mode = "overwrite" if rebuild else "append"

        log.info(f"[{fact_name}] Writing to GOLD.{gold_table} (mode={mode})...")
        write_to_snowflake(gold_df, gold_table, schema="GOLD", mode=mode)

        log.info(f"[SUCCESS] [{fact_name}] Fact build complete ({gold_df.count()} rows)")
        return gold_df.count(), "SUCCESS"

    except Exception as e:
        log.error(f"[ERROR] [{fact_name}] Fact build failed: {e}", exc_info=True)
        return 0, "FAILED"


def run_pipeline(args):
    """Execute Gold modeling pipeline."""

    # Setup logging
    log = setup_logging("gold_modeling", debug=args.debug)

    log_section(log, "GOLD MODELING PIPELINE")

    # Validate Snowflake configuration
    try:
        validate_snowflake_config()
    except ValueError as e:
        log.error(f"[ERROR] Configuration error: {e}")
        log.error("Gold modeling requires Snowflake connection")
        return 1

    # Determine what to build
    build_dimensions = args.all or args.dimensions
    build_facts = args.all or args.facts

    rebuild_dims = args.rebuild or GOLD_REBUILD_DIMS
    rebuild_facts = args.rebuild or GOLD_REBUILD_FACTS

    log.info(f"Build plan: dimensions={build_dimensions}, facts={build_facts}")
    log.info(f"Rebuild mode: dimensions={rebuild_dims}, facts={rebuild_facts}")

    # Initialize Spark
    log.info("Initializing Spark session...")
    spark = get_spark_session("GoldModeling")

    # Track metrics
    overall_metrics = PipelineMetrics("gold_modeling")
    overall_metrics.start()

    successful = 0
    failed = 0

    # Build dimensions
    if build_dimensions:
        log_section(log, "BUILDING DIMENSIONS")

        for dim_name, (silver_table, transformer_class) in DIMENSIONS.items():
            log_section(log, f"DIMENSION: {dim_name.upper()}")

            row_count, status = build_dimension(
                spark, dim_name, silver_table, transformer_class, rebuild_dims, log
            )

            if status == "SUCCESS":
                successful += 1
            else:
                failed += 1

    # Build facts
    if build_facts:
        log_section(log, "BUILDING FACTS")

        for fact_name, (silver_table, transformer_class) in FACTS.items():
            log_section(log, f"FACT: {fact_name.upper()}")

            row_count, status = build_fact(
                spark, fact_name, silver_table, transformer_class, rebuild_facts, log
            )

            if status == "SUCCESS":
                successful += 1
            else:
                failed += 1

    # Cleanup
    stop_spark_session()

    # Overall summary
    overall_metrics.finish("SUCCESS" if failed == 0 else "PARTIAL")

    log_section(log, "PIPELINE SUMMARY")
    log.info(f"[SUCCESS] Successful: {successful}")
    log.info(f"[ERROR] Failed: {failed}")
    log.info(f"Duration: {overall_metrics.metrics['duration_sec']:.2f}s")

    overall_metrics.save()

    return 0 if failed == 0 else 1


def main():
    """Main entry point."""
    args = parse_args()

    try:
        exit_code = run_pipeline(args)
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n[WARNING] Pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
