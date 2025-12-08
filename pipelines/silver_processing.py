"""
Silver Processing Pipeline - Transform Bronze data to Silver layer.

Usage:
    python pipelines/silver_processing.py --resources employees,projects
    python pipelines/silver_processing.py --all
    python pipelines/silver_processing.py --all --lookback-days 7
    python pipelines/silver_processing.py --resources employees --debug
"""
import argparse
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import datetime, timedelta

from src.common.config import (
    validate_snowflake_config,
    SILVER_LOOKBACK_DAYS,
    SILVER_MERGE_ENABLED
)
from src.common.logging_utils import setup_logging, PipelineMetrics, log_section
from src.common.spark_utils import get_spark_session, stop_spark_session
from src.common.snowflake_utils import read_from_snowflake, write_to_snowflake, merge_to_snowflake
from src.extractors.ceipal.config import list_resources

# Import transformers
from src.transformers.silver.employees import EmployeesTransformer
from src.transformers.silver.projects import ProjectsTransformer
from src.transformers.silver.placements import PlacementsTransformer
from src.transformers.silver.clients import ClientsTransformer
from src.transformers.silver.vendors import VendorsTransformer
from src.transformers.silver.expenses import ExpensesTransformer
from src.transformers.silver.invoices import InvoicesTransformer


# Transformer registry
TRANSFORMERS = {
    "employees": EmployeesTransformer,
    "projects": ProjectsTransformer,
    "placements": PlacementsTransformer,
    "clients": ClientsTransformer,
    "vendors": VendorsTransformer,
    "expenses": ExpensesTransformer,
    "invoices": InvoicesTransformer,
}


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="CEIPAL Silver Processing Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all resources
  python pipelines/silver_processing.py --all

  # Process specific resources
  python pipelines/silver_processing.py --resources employees,projects

  # Full rebuild (process all data, not just recent)
  python pipelines/silver_processing.py --all --full-rebuild

  # Custom lookback window
  python pipelines/silver_processing.py --all --lookback-days 30

  # Disable MERGE (use overwrite instead)
  python pipelines/silver_processing.py --resources employees --no-merge

  # Debug mode
  python pipelines/silver_processing.py --resources employees --debug
        """
    )

    # Resource selection
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--all",
        action="store_true",
        help="Process all available resources"
    )
    group.add_argument(
        "--resources",
        type=str,
        help="Comma-separated list of resources (e.g., employees,projects)"
    )

    # Processing options
    parser.add_argument(
        "--full-rebuild",
        action="store_true",
        help="Process all data (ignore lookback window)"
    )

    parser.add_argument(
        "--lookback-days",
        type=int,
        default=SILVER_LOOKBACK_DAYS,
        help=f"Process data from last N days (default: {SILVER_LOOKBACK_DAYS})"
    )

    parser.add_argument(
        "--no-merge",
        action="store_true",
        help="Disable MERGE, use overwrite mode instead"
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging"
    )

    return parser.parse_args()


def process_resource(
    spark,
    resource: str,
    lookback_days: int,
    full_rebuild: bool,
    use_merge: bool,
    debug: bool
):
    """
    Process a single resource from Bronze to Silver.

    Args:
        spark: SparkSession
        resource: Resource name
        lookback_days: Lookback window in days
        full_rebuild: Process all data
        use_merge: Use MERGE instead of overwrite
        debug: Debug mode

    Returns:
        Tuple of (input_count, output_count, status)
    """
    log = setup_logging(f"silver_{resource}", debug=debug)

    log_section(log, f"PROCESSING: {resource.upper()}")

    try:
        # Get transformer
        if resource not in TRANSFORMERS:
            log.error(f"No transformer found for resource '{resource}'")
            return (0, 0, "FAILED")

        transformer_class = TRANSFORMERS[resource]
        transformer = transformer_class(spark)

        # Read from Bronze
        bronze_table = f"CEIPAL_{resource.upper()}_BRONZE"

        log.info(f"[{resource}] Reading from RAW.{bronze_table}...")

        # Build filter clause for windowed read
        filter_clause = None
        if not full_rebuild:
            lookback_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
            filter_clause = f"LOADED_AT >= '{lookback_date}'"
            log.info(f"[{resource}] Using windowed read (lookback: {lookback_days} days)")

        bronze_df = read_from_snowflake(
            spark=spark,
            table=bronze_table,
            schema="RAW",
            filter_clause=filter_clause
        )

        input_count = bronze_df.count()

        if input_count == 0:
            log.warning(f"[{resource}] No data in Bronze layer")
            return (0, 0, "NO_DATA")

        log.info(f"[{resource}] Read {input_count} records from Bronze")

        # Transform
        log.info(f"[{resource}] Applying transformations...")
        silver_df = transformer.run(bronze_df)

        output_count = silver_df.count()
        log.info(f"[{resource}] Transformation complete ({output_count} rows)")

        # Write to Silver
        silver_table = f"CEIPAL_{resource.upper()}"

        if use_merge and not full_rebuild:
            log.info(f"[{resource}] Merging to SILVER.{silver_table}...")
            merge_keys = transformer.get_merge_keys()

            merge_to_snowflake(
                spark=spark,
                df=silver_df,
                table=silver_table,
                merge_keys=merge_keys,
                target_schema="SILVER",
                staging_schema="STAGING"
            )
        else:
            mode = "overwrite" if full_rebuild else "append"
            log.info(f"[{resource}] Writing to SILVER.{silver_table} (mode={mode})...")

            write_to_snowflake(
                df=silver_df,
                table=silver_table,
                schema="SILVER",
                mode=mode
            )

        log.info(f"[SUCCESS] [{resource}] Silver processing complete")

        return (input_count, output_count, "SUCCESS")

    except Exception as e:
        log.error(f"[ERROR] [{resource}] Processing failed: {e}", exc_info=debug)
        return (0, 0, "FAILED")


def run_pipeline(args):
    """Execute Silver processing pipeline."""

    # Setup logging
    log = setup_logging("silver_processing", debug=args.debug)

    log_section(log, "SILVER PROCESSING PIPELINE")

    # Validate Snowflake configuration
    try:
        validate_snowflake_config()
    except ValueError as e:
        log.error(f"[ERROR] Configuration error: {e}")
        log.error("Silver processing requires Snowflake connection")
        return 1

    # Determine resources to process
    if args.all:
        resources = list_resources()
        log.info(f"Processing ALL resources: {', '.join(resources)}")
    else:
        resources = [r.strip() for r in args.resources.split(',')]
        log.info(f"Processing resources: {', '.join(resources)}")

    # Processing options
    use_merge = SILVER_MERGE_ENABLED and not args.no_merge
    log.info(f"Options: full_rebuild={args.full_rebuild}, lookback_days={args.lookback_days}, merge={use_merge}")

    # Initialize Spark
    log.info("Initializing Spark session...")
    spark = get_spark_session("SilverProcessing")

    # Process each resource
    overall_metrics = PipelineMetrics("silver_processing")
    overall_metrics.start()

    successful = 0
    failed = 0

    for resource in resources:
        resource_metrics = PipelineMetrics("silver_processing", resource=resource)
        resource_metrics.start()

        input_count, output_count, status = process_resource(
            spark=spark,
            resource=resource,
            lookback_days=args.lookback_days,
            full_rebuild=args.full_rebuild,
            use_merge=use_merge,
            debug=args.debug
        )

        resource_metrics.set_input_count(input_count)
        resource_metrics.set_output_count(output_count)
        resource_metrics.finish(status)

        log.info(f"[{resource}] {resource_metrics.get_summary()}")

        if status == "SUCCESS":
            successful += 1
        elif status == "FAILED":
            failed += 1

        resource_metrics.save()

    # Cleanup
    stop_spark_session()

    # Overall summary
    overall_metrics.finish("SUCCESS" if failed == 0 else "PARTIAL")

    log_section(log, "PIPELINE SUMMARY")
    log.info(f"Total resources: {len(resources)}")
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
