"""
Bronze loader with type safety - handles dual-mode operation (JSON or Snowflake).
Implements idempotency via record_hash deduplication.
"""
import logging
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Any, Set
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType

from src.common.config import settings
from src.common.enums import OutputMode
from src.common.snowflake_utils import write_to_snowflake
from src.extractors.common.utils import compute_record_hash, chunk_list

log = logging.getLogger(__name__)


class BronzeLoader:
    """
    Loads raw data to Bronze layer with dual-mode support.

    Modes:
        - JSON: Writes to output/data/*.json (for testing, via --json-output flag)
        - Snowflake: Writes to RAW.CEIPAL_*_BRONZE tables (default)

    Features:
        - Idempotent loading via record_hash deduplication
        - Checkpoint-based tracking of loaded records
        - Batch writing for Snowflake
        - Timestamped JSON files for testing

    Attributes:
        output_mode: Either 'json' or 'snowflake'
        spark: SparkSession (required for Snowflake mode)
    """

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        output_mode: str = "snowflake"
    ):
        """
        Initialize Bronze loader.

        Args:
            spark: SparkSession (only needed for Snowflake mode)
            output_mode: 'json' or 'snowflake' (defaults to 'snowflake')

        Raises:
            ValueError: If Snowflake mode selected without Spark session
        """
        # Validate output mode
        try:
            self.output_mode = OutputMode(output_mode)
        except ValueError:
            valid_modes = ", ".join(m.value for m in OutputMode)
            raise ValueError(f"Invalid output_mode: {output_mode}. Valid: {valid_modes}")

        # Validate Spark is provided when needed
        if self.output_mode == OutputMode.SNOWFLAKE and spark is None:
            raise ValueError("Spark session required for Snowflake mode")

        self.spark = spark
        log.info(f"BronzeLoader initialized (mode={self.output_mode.value})")

    def _get_table_name(self, resource: str) -> str:
        """
        Get Bronze table name for resource.

        Args:
            resource: Resource name (e.g., 'employees')

        Returns:
            Table name (e.g., 'CEIPAL_EMPLOYEES_BRONZE')
        """
        return f"CEIPAL_{resource.upper()}_BRONZE"

    def _get_json_path(self, resource: str) -> Path:
        """
        Get JSON output path for resource with timestamp.

        Args:
            resource: Resource name

        Returns:
            Path to JSON file
        """
        output_dir = Path("output/data")
        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return output_dir / f"ceipal_{resource}_{timestamp}.json"

    def _load_checkpoint(self, resource: str) -> Set[str]:
        """
        Load checkpoint of already-loaded record hashes.

        Args:
            resource: Resource name

        Returns:
            Set of record hash strings
        """
        if not settings.checkpoint_enabled:
            return set()

        checkpoint_file = settings.checkpoint_dir / f"bronze_{resource}_hashes.json"

        if not checkpoint_file.exists():
            return set()

        try:
            with open(checkpoint_file, 'r') as f:
                data = json.load(f)
                return set(data.get('hashes', []))
        except Exception as e:
            log.warning(f"Failed to load checkpoint: {e}")
            return set()

    def _save_checkpoint(self, resource: str, hashes: Set[str]) -> None:
        """
        Save checkpoint of loaded record hashes.

        Args:
            resource: Resource name
            hashes: Set of record hashes to save
        """
        if not settings.checkpoint_enabled:
            return

        checkpoint_file = settings.checkpoint_dir / f"bronze_{resource}_hashes.json"

        try:
            checkpoint_file.parent.mkdir(parents=True, exist_ok=True)

            with open(checkpoint_file, 'w') as f:
                json.dump({
                    'resource': resource,
                    'updated_at': datetime.utcnow().isoformat(),
                    'count': len(hashes),
                    'hashes': list(hashes)
                }, f)

            log.debug(f"Checkpoint saved: {len(hashes)} hashes")

        except Exception as e:
            log.warning(f"Failed to save checkpoint: {e}")

    def _deduplicate_records(
        self,
        records: List[Dict[str, Any]],
        resource: str
    ) -> List[Dict[str, Any]]:
        """
        Remove duplicate records based on record_hash.

        Uses checkpoint to skip already-loaded records for idempotency.

        Args:
            records: List of records to deduplicate
            resource: Resource name

        Returns:
            List of new (non-duplicate) records
        """
        if not records:
            return []

        # Load existing hashes from checkpoint
        existing_hashes = self._load_checkpoint(resource)

        log.info(f"[{resource}] Checkpoint has {len(existing_hashes)} existing hashes")

        # Compute hashes for new records
        new_records: List[Dict[str, Any]] = []
        new_hashes: Set[str] = set()
        duplicates = 0

        for record in records:
            record_hash = compute_record_hash(record)
            record['_record_hash'] = record_hash

            # Skip if already loaded
            if record_hash in existing_hashes:
                duplicates += 1
                continue

            # Skip if duplicate in current batch
            if record_hash in new_hashes:
                duplicates += 1
                continue

            new_records.append(record)
            new_hashes.add(record_hash)

        log.info(
            f"[{resource}] Deduplication: {len(records)} input → "
            f"{len(new_records)} new, {duplicates} duplicates"
        )

        # Update checkpoint with new hashes
        if new_records:
            updated_hashes = existing_hashes.union(new_hashes)
            self._save_checkpoint(resource, updated_hashes)

        return new_records

    def _write_to_json(self, records: List[Dict[str, Any]], resource: str) -> None:
        """
        Write records to JSON file for testing.

        Args:
            records: Records to write
            resource: Resource name
        """
        if not records:
            log.info(f"[{resource}] No records to write")
            return

        json_path = self._get_json_path(resource)

        log.info(f"[{resource}] Writing {len(records)} records to {json_path}")

        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump({
                'resource': resource,
                'extracted_at': datetime.utcnow().isoformat(),
                'count': len(records),
                'records': records
            }, f, indent=2, default=str)

        log.info(f"[SUCCESS] [{resource}] JSON write complete: {json_path}")

    def _write_to_snowflake(
        self,
        records: List[Dict[str, Any]],
        resource: str
    ) -> None:
        """
        Write records to Snowflake RAW.CEIPAL_*_BRONZE table.

        Schema:
            - ID: VARCHAR (primary key from source)
            - DATA: VARIANT (full JSON record)
            - RECORD_HASH: VARCHAR (for deduplication)
            - LOADED_AT: TIMESTAMP_NTZ

        Args:
            records: Records to write
            resource: Resource name
        """
        if not records:
            log.info(f"[{resource}] No records to write")
            return

        table_name = self._get_table_name(resource)

        log.info(f"[{resource}] Preparing {len(records)} records for Snowflake")

        # Define schema for Bronze table
        schema = StructType([
            StructField("ID", StringType(), False),
            StructField("DATA", StringType(), False),
            StructField("RECORD_HASH", StringType(), False),
            StructField("LOADED_AT", StringType(), False)
        ])

        # Write in batches
        total_written = 0
        batches = chunk_list(records, settings.bronze_batch_size)

        for i, batch in enumerate(batches, 1):
            log.info(
                f"[{resource}] Writing batch {i}/{len(batches)} "
                f"({len(batch)} records)"
            )

            # Convert batch to Bronze schema
            batch_records = [
                {
                    'ID': str(record.get('id', '')),
                    'DATA': json.dumps(record, default=str),
                    'RECORD_HASH': record.get('_record_hash', ''),
                    'LOADED_AT': datetime.utcnow().isoformat()
                }
                for record in batch
            ]

            batch_df = self.spark.createDataFrame(batch_records, schema)

            write_to_snowflake(
                df=batch_df,
                table=table_name,
                schema="RAW",
                mode="append"
            )

            total_written += len(batch)

        log.info(f"[SUCCESS] [{resource}] Snowflake write complete: {total_written} records")

    def load(
        self,
        records: List[Dict[str, Any]],
        resource: str,
        skip_dedup: bool = False
    ) -> None:
        """
        Load records to Bronze layer with automatic deduplication.

        Args:
            records: List of records to load
            resource: Resource name (e.g., 'employees')
            skip_dedup: Skip deduplication (useful for testing)

        Raises:
            ValueError: If output_mode is invalid
        """
        if not records:
            log.warning(f"[{resource}] No records provided")
            return

        log.info(
            f"[{resource}] Loading {len(records)} records to Bronze "
            f"(mode={self.output_mode.value})"
        )

        # Deduplication (unless skipped)
        if not skip_dedup:
            records = self._deduplicate_records(records, resource)

        if not records:
            log.info(f"[{resource}] All records were duplicates, nothing to load")
            return

        # Route to appropriate output
        if self.output_mode == OutputMode.JSON:
            self._write_to_json(records, resource)
        elif self.output_mode == OutputMode.SNOWFLAKE:
            self._write_to_snowflake(records, resource)
        else:
            raise ValueError(f"Invalid output_mode: {self.output_mode}")

        log.info(f"[SUCCESS] [{resource}] Bronze load complete")

    def load_batch(
        self,
        resources_data: Dict[str, List[Dict[str, Any]]],
        skip_dedup: bool = False
    ) -> None:
        """
        Load multiple resources in batch.

        Args:
            resources_data: Dictionary mapping resource names to record lists
            skip_dedup: Skip deduplication for all resources

        Raises:
            Exception: If any resource load fails
        """
        log.info(f"Loading batch of {len(resources_data)} resources to Bronze")

        for resource, records in resources_data.items():
            try:
                self.load(records, resource, skip_dedup=skip_dedup)
            except Exception as e:
                log.error(f"[ERROR] [{resource}] Failed to load: {e}")
                raise

        log.info("[SUCCESS] Batch load complete")
