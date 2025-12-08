"""
Master Pipeline - Orchestrates Bronze → Silver → Gold.

Usage:
    python pipelines/run_full_pipeline.py
    python pipelines/run_full_pipeline.py --resources employees,projects
    python pipelines/run_full_pipeline.py --skip-bronze
    python pipelines/run_full_pipeline.py --skip-gold
"""
import argparse
import sys
import subprocess
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.common.logging_utils import setup_logging, PipelineMetrics, log_section


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="CEIPAL Full ETL Pipeline (Bronze → Silver → Gold)",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "--resources",
        type=str,
        help="Comma-separated list of resources (default: all)"
    )

    parser.add_argument("--skip-bronze", action="store_true", help="Skip Bronze ingestion")
    parser.add_argument("--skip-silver", action="store_true", help="Skip Silver processing")
    parser.add_argument("--skip-gold", action="store_true", help="Skip Gold modeling")

    parser.add_argument("--json-output", action="store_true", help="Bronze: output to JSON")
    parser.add_argument("--full-rebuild", action="store_true", help="Full rebuild (all layers)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    return parser.parse_args()


def run_command(cmd: list, log) -> int:
    """Run subprocess command."""
    log.info(f"Executing: {' '.join(cmd)}")

    try:
        result = subprocess.run(cmd, check=False, capture_output=False)
        return result.returncode
    except Exception as e:
        log.error(f"Command failed: {e}")
        return 1


def run_pipeline(args):
    """Execute full ETL pipeline."""

    # Setup logging
    log = setup_logging("full_pipeline", debug=args.debug)

    log_section(log, "FULL ETL PIPELINE")

    # Track overall metrics
    overall_metrics = PipelineMetrics("full_pipeline")
    overall_metrics.start()

    # Determine resource scope
    resource_arg = f"--resources {args.resources}" if args.resources else "--all"

    # Step 1: Bronze Ingestion
    if not args.skip_bronze:
        log_section(log, "STEP 1: BRONZE INGESTION")

        bronze_cmd = [
            sys.executable,
            "pipelines/bronze_ingestion.py",
            *resource_arg.split()
        ]

        if args.json_output:
            bronze_cmd.append("--json-output")

        if args.debug:
            bronze_cmd.append("--debug")

        exit_code = run_command(bronze_cmd, log)

        if exit_code != 0:
            log.error("[ERROR] Bronze ingestion failed, stopping pipeline")
            return exit_code
    else:
        log.info("[SKIP] Skipping Bronze ingestion")

    # Step 2: Silver Processing
    if not args.skip_silver:
        log_section(log, "STEP 2: SILVER PROCESSING")

        silver_cmd = [
            sys.executable,
            "pipelines/silver_processing.py",
            *resource_arg.split()
        ]

        if args.full_rebuild:
            silver_cmd.append("--full-rebuild")

        if args.debug:
            silver_cmd.append("--debug")

        exit_code = run_command(silver_cmd, log)

        if exit_code != 0:
            log.error("[ERROR] Silver processing failed, stopping pipeline")
            return exit_code
    else:
        log.info("[SKIP] Skipping Silver processing")

    # Step 3: Gold Modeling
    if not args.skip_gold:
        log_section(log, "STEP 3: GOLD MODELING")

        gold_cmd = [
            sys.executable,
            "pipelines/gold_modeling.py",
            "--all"
        ]

        if args.full_rebuild:
            gold_cmd.append("--rebuild")

        if args.debug:
            gold_cmd.append("--debug")

        exit_code = run_command(gold_cmd, log)

        if exit_code != 0:
            log.error("[ERROR] Gold modeling failed")
            return exit_code
    else:
        log.info("[SKIP] Skipping Gold modeling")

    # Overall summary
    overall_metrics.finish("SUCCESS")

    log_section(log, "PIPELINE COMPLETE")
    log.info(f"[SUCCESS] Full pipeline executed successfully")
    log.info(f"Duration: {overall_metrics.metrics['duration_sec']:.2f}s")

    overall_metrics.save()

    return 0


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
