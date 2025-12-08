"""Extract from CEIPAL API and load to Bronze layer"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.common.config import validate_config
from src.common.logging_utils import setup_logging, PipelineMetrics, log_section
from src.common.spark_utils import get_spark_session, stop_spark_session
from src.extractors.ceipal.api_client import CeipalClient
from src.extractors.ceipal.config import list_resources
from src.extractors.common.bronze_loader import BronzeLoader


def parse_args():
    parser = argparse.ArgumentParser(description="CEIPAL Bronze Ingestion Pipeline")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--all", action="store_true", help="Extract all resources")
    group.add_argument("--resources", type=str, help="Comma-separated list (e.g. employees,projects)")
    parser.add_argument("--json-output", action="store_true", help="Output to JSON instead of Snowflake")
    parser.add_argument("--sample", type=int, metavar="N", help="Fetch only first N records")
    parser.add_argument("--employee-number", type=str, metavar="NUM", help="Fetch single employee by employee number (e.g., ES-10065)")
    parser.add_argument("--skip-dedup", action="store_true", help="Skip deduplication")
    parser.add_argument("--with-details", action="store_true", help="Fetch details for each record (overrides config)")
    parser.add_argument("--debug", action="store_true", help="Debug logging")
    return parser.parse_args()


def run_pipeline(args):
    log = setup_logging("bronze_ingestion", debug=args.debug)

    log_section(log, "BRONZE INGESTION PIPELINE")

    try:
        validate_config()
    except ValueError as e:
        log.error(f"[ERROR] Configuration error: {e}")
        return 1

    if args.all:
        resources = list_resources()
        log.info(f"Extracting ALL resources: {', '.join(resources)}")
    else:
        resources = [r.strip() for r in args.resources.split(',')]
        log.info(f"Extracting resources: {', '.join(resources)}")

    log.info("Initializing components...")

    try:
        client = CeipalClient()

        # Only create Spark session if needed (Snowflake mode)
        if args.json_output:
            log.info("JSON mode - skipping Spark initialization")
            loader = BronzeLoader(spark=None, output_mode="json")
        else:
            log.info("Snowflake mode - initializing Spark session")
            spark = get_spark_session("BronzeIngestion")
            loader = BronzeLoader(spark, output_mode="snowflake")

        log.info(f"[SUCCESS] Components initialized (output_mode={loader.output_mode})")

    except Exception as e:
        log.error(f"[ERROR] Failed to initialize components: {e}")
        return 1

    overall_metrics = PipelineMetrics("bronze_ingestion")
    overall_metrics.start()

    successful = 0
    failed = 0

    for resource in resources:
        resource_metrics = PipelineMetrics("bronze_ingestion", resource=resource)
        resource_metrics.start()

        log_section(log, f"RESOURCE: {resource.upper()}")

        try:
            log.info(f"[{resource}] Extracting from CEIPAL API...")

            # Check for single employee fetch
            if args.employee_number and resource == 'employees':
                log.info(f"[{resource}] Fetching single employee: {args.employee_number}")
                # Note: fetch_detail always gets detail endpoint, --with-details flag doesn't apply here
                detail = client.fetch_detail(resource, args.employee_number)
                if detail:
                    from datetime import datetime
                    detail['_extracted_at'] = datetime.utcnow().isoformat()
                    detail['_resource'] = resource
                    records = [detail]
                else:
                    log.warning(f"[{resource}] Employee {args.employee_number} not found")
                    records = []
            else:
                filters = {}
                if args.sample:
                    log.info(f"[{resource}] Sample mode: limiting to {args.sample} records")
                    filters['limit'] = args.sample

                # Override fetch_details if --with-details flag is used
                fetch_details = True if args.with_details else None
                records = client.extract_resource(resource, filters=filters, fetch_details=fetch_details)

            resource_metrics.set_input_count(len(records))

            if not records:
                log.warning(f"[{resource}] No records extracted")
                resource_metrics.finish("NO_DATA")
                continue

            log.info(f"[{resource}] Extracted {len(records)} records")

            log.info(f"[{resource}] Loading to Bronze layer...")
            loader.load(records=records, resource=resource, skip_dedup=args.skip_dedup)

            resource_metrics.set_output_count(len(records))
            resource_metrics.finish("SUCCESS")

            log.info(f"[SUCCESS] [{resource}] {resource_metrics.get_summary()}")

            successful += 1

        except Exception as e:
            log.error(f"[ERROR] [{resource}] Pipeline failed: {e}", exc_info=args.debug)
            resource_metrics.add_error(str(e))
            resource_metrics.finish("FAILED")
            failed += 1
            if not args.all:
                break

        finally:
            resource_metrics.save()

    # Only stop Spark if it was started
    if not args.json_output:
        stop_spark_session()

    overall_metrics.finish("SUCCESS" if failed == 0 else "PARTIAL")

    log_section(log, "PIPELINE SUMMARY")
    log.info(f"Total resources: {len(resources)}")
    log.info(f"[SUCCESS] Successful: {successful}")
    log.info(f"[ERROR] Failed: {failed}")
    log.info(f"Duration: {overall_metrics.metrics['duration_sec']:.2f}s")
    log.info(f"Output mode: {loader.output_mode}")

    if loader.output_mode == "json":
        log.info("JSON files written to: output/data/")

    overall_metrics.save()

    return 0 if failed == 0 else 1


def main():
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
