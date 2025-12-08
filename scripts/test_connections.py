"""
Test script for CEIPAL and Snowflake connections
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def test_ceipal():
    print("\n" + "=" * 60)
    print("TESTING CEIPAL API CONNECTION")
    print("=" * 60)

    try:
        from src.common.config import validate_config
        from src.extractors.ceipal.auth import TokenManager

        print("\n1. Validating configuration...")
        validate_config()
        print("[OK] Configuration valid")

        print("\n2. Testing authentication...")
        token_manager = TokenManager()
        token = token_manager.get_token()
        print(f"[OK] Authentication successful")
        print(f"   Token: {token[:20]}...")

        print("\n3. Testing API call (fetch employees list)...")
        from src.extractors.ceipal.api_client import CeipalClient

        client = CeipalClient()
        employees = client.fetch_list("employees", filters={"limit": 5})
        print(f"[OK] API call successful")
        print(f"   Fetched {len(employees)} sample employees")

        print("\n" + "=" * 60)
        print("[SUCCESS] CEIPAL API: ALL TESTS PASSED")
        print("=" * 60)

        return True

    except Exception as e:
        print(f"\n[FAIL] CEIPAL API test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_snowflake():
    print("\n" + "=" * 60)
    print("TESTING SNOWFLAKE CONNECTION")
    print("=" * 60)

    try:
        from src.common.config import validate_snowflake_config, is_snowflake_configured
        import snowflake.connector
        from src.common.config import (
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_USER,
            SNOWFLAKE_PASSWORD,
            SNOWFLAKE_WAREHOUSE,
            SNOWFLAKE_DATABASE,
            SNOWFLAKE_ROLE
        )

        print("\n1. Checking configuration...")
        if not is_snowflake_configured():
            print("[WARN] Snowflake not configured (credentials missing in .env)")
            print("   This is OK for JSON-output mode testing")
            return None

        validate_snowflake_config()
        print("[OK] Configuration valid")

        print("\n2. Testing connection...")
        conn = snowflake.connector.connect(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            role=SNOWFLAKE_ROLE
        )
        print("[OK] Connection successful")

        print("\n3. Testing query (SHOW SCHEMAS)...")
        cursor = conn.cursor()
        cursor.execute(f"SHOW SCHEMAS IN DATABASE {SNOWFLAKE_DATABASE}")
        schemas = cursor.fetchall()
        print(f"[OK] Query successful")
        print(f"   Found {len(schemas)} schemas:")
        for schema in schemas:
            print(f"     - {schema[1]}")

        cursor.close()
        conn.close()

        print("\n" + "=" * 60)
        print("[SUCCESS] SNOWFLAKE: ALL TESTS PASSED")
        print("=" * 60)

        return True

    except Exception as e:
        print(f"\n[FAIL] Snowflake test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def parse_args():
    parser = argparse.ArgumentParser(description="Test connections")
    parser.add_argument("--ceipal-only", action="store_true", help="Test CEIPAL only")
    parser.add_argument("--snowflake-only", action="store_true", help="Test Snowflake only")
    return parser.parse_args()


def main():
    args = parse_args()

    print("=" * 60)
    print("CONNECTION TESTS")
    print("=" * 60)

    results = {}

    if not args.snowflake_only:
        results['ceipal'] = test_ceipal()

    if not args.ceipal_only:
        results['snowflake'] = test_snowflake()

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    for service, result in results.items():
        if result is True:
            print(f"[PASS] {service.upper()}: PASS")
        elif result is False:
            print(f"[FAIL] {service.upper()}: FAIL")
        else:
            print(f"[WARN] {service.upper()}: NOT CONFIGURED")

    if any(r is False for r in results.values()):
        print("\n[FAIL] Some tests failed")
        sys.exit(1)
    else:
        print("\n[SUCCESS] All configured services passed")
        sys.exit(0)


if __name__ == "__main__":
    main()
