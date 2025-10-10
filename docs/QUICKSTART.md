# Quick Start Guide - CEIPAL PySpark ETL

Get up and running in 10 minutes!

## Prerequisites

- Python 3.8+
- Java 8 or 11
- Snowflake account
- CEIPAL API credentials

## Installation

```bash
# 1. Install dependencies
pip install -r requirements-spark.txt

# 2. Configure environment
cp .env.example .env
# Edit .env with your credentials

# 3. Setup Snowflake
# Run snowflake_ddl.sql in Snowflake UI or:
snowsql -f snowflake_ddl.sql
```

## Run Your First ETL

### Test with Small Dataset

```bash
# Load 50 employees (no details, fast)
python dump_ceipal_employees_spark.py \
  --employees \
  --limit 50 \
  --max-pages 1
```

### Load with Details

```bash
# Load employees with full details
python dump_ceipal_employees_spark.py \
  --employees \
  --with-details \
  --workers 4 \
  --rps 2.0
```

### Verify in Snowflake

```sql
-- Check loaded data
SELECT COUNT(*) FROM CEIPAL_DW.RAW.EMPLOYEES;

-- View recent records
SELECT * FROM CEIPAL_DW.RAW.EMPLOYEES
ORDER BY load_timestamp DESC
LIMIT 10;
```

## Incremental Load

```bash
# First run: full load
python dump_ceipal_employees_spark.py --employees --load-strategy append

# Subsequent runs: merge updates
python dump_ceipal_employees_spark.py --employees --load-strategy merge
```

## Load All Entities

```bash
python dump_ceipal_employees_spark.py --all --with-details
```

## Troubleshooting

### If PySpark fails:
```bash
# Use original script (fallback to JSON)
python dump_ceipal_employees.py --employees
```

### If Snowflake connection fails:
```bash
# Test connection
snowsql -a your-account -u your-user
```

### If API rate limited:
```bash
# Reduce request rate
python dump_ceipal_employees_spark.py \
  --employees \
  --rps 0.5 \
  --workers 2
```

## Next Steps

1. Review full documentation: `README_SPARK.md`
2. Explore Snowflake tables and views
3. Set up scheduled runs (cron/Airflow)
4. Create BI dashboards

## Architecture Overview

```
CEIPAL API → PySpark → Snowflake
                          ├─ RAW (source of truth)
                          ├─ STAGING (temp)
                          ├─ WAREHOUSE (star schema)
                          └─ ANALYTICS (views)
```

## Files Reference

| File | Purpose |
|------|---------|
| `dump_ceipal_employees_spark.py` | Main ETL script |
| `snowflake_ddl.sql` | Database schema |
| `requirements-spark.txt` | Python dependencies |
| `.env` | Configuration |
| `README_SPARK.md` | Full documentation |

## Key Commands

```bash
# Load employees
python dump_ceipal_employees_spark.py --employees

# Load projects with details
python dump_ceipal_employees_spark.py --projects --with-details

# Load clients
python dump_ceipal_employees_spark.py --clients

# Load all entities
python dump_ceipal_employees_spark.py --all

# Use merge strategy (incremental)
python dump_ceipal_employees_spark.py --employees --load-strategy merge

# Debug mode
python dump_ceipal_employees_spark.py --employees --debug
```

## Common Issues

**Q: Java not found?**
A: `export JAVA_HOME=$(/usr/libexec/java_home)`

**Q: Snowflake connection timeout?**
A: Check account name format and firewall

**Q: PySpark out of memory?**
A: Reduce batch size or increase `SPARK_DRIVER_MEMORY`

**Q: API rate limit errors?**
A: Lower `--rps` value and `--workers`

---

For detailed information, see `README_SPARK.md`
