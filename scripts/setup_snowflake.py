"""
Setup Snowflake database - creates schemas and tables.

Usage:
    python scripts/setup_snowflake.py
    python scripts/setup_snowflake.py --dry-run
"""
import argparse
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import snowflake.connector
from src.common.config import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_ROLE,
    validate_snowflake_config
)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Setup Snowflake database")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL without executing")
    return parser.parse_args()


def get_connection():
    """Create Snowflake connection."""
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE
    )


def execute_sql_file(conn, file_path: Path, dry_run: bool = False):
    """Execute SQL file."""
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Executing: {file_path.name}")
    print("=" * 60)

    with open(file_path, 'r') as f:
        sql = f.read()

    if dry_run:
        print(sql)
        return

    cursor = conn.cursor()

    # Split by semicolon and execute each statement
    statements = [s.strip() for s in sql.split(';') if s.strip() and not s.strip().startswith('--')]

    for stmt in statements:
        try:
            print(f"Executing: {stmt[:80]}...")
            cursor.execute(stmt)
            print("[SUCCESS] Success")
        except Exception as e:
            print(f"[ERROR] Error: {e}")
            raise

    cursor.close()


def main():
    """Main entry point."""
    args = parse_args()

    print("=" * 60)
    print("SNOWFLAKE DATABASE SETUP")
    print("=" * 60)

    # Validate configuration
    try:
        validate_snowflake_config()
    except ValueError as e:
        print(f"[ERROR] Configuration error: {e}")
        sys.exit(1)

    print(f"\nTarget database: {SNOWFLAKE_DATABASE}")
    print(f"Account: {SNOWFLAKE_ACCOUNT}")
    print(f"User: {SNOWFLAKE_USER}")

    if args.dry_run:
        print("\n[WARNING] DRY RUN MODE - No changes will be made")

    # Connect to Snowflake
    if not args.dry_run:
        print("\nConnecting to Snowflake...")
        try:
            conn = get_connection()
            print("[SUCCESS] Connected")
        except Exception as e:
            print(f"[ERROR] Connection failed: {e}")
            sys.exit(1)
    else:
        conn = None

    # Execute SQL files in order
    sql_dir = Path("sql")

    sql_files = [
        sql_dir / "00_setup" / "create_schemas.sql",
        sql_dir / "10_bronze" / "create_bronze_tables.sql",
        sql_dir / "30_silver" / "create_all_silver_tables.sql",
        sql_dir / "40_gold" / "create_all_gold_tables.sql",
    ]

    try:
        for sql_file in sql_files:
            if not sql_file.exists():
                print(f"[WARNING] File not found: {sql_file}")
                continue

            execute_sql_file(conn, sql_file, dry_run=args.dry_run)

        print("\n" + "=" * 60)
        print("[SUCCESS] SETUP COMPLETE")
        print("=" * 60)

    except Exception as e:
        print(f"\n[ERROR] Setup failed: {e}")
        sys.exit(1)

    finally:
        if conn:
            conn.close()
            print("\nConnection closed")


if __name__ == "__main__":
    main()
