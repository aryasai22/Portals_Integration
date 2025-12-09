# CEIPAL to Snowflake ETL Pipeline

Extracts workforce data from CEIPAL API and loads it into Snowflake using a three-layer architecture (Bronze/Silver/Gold). Supports local testing with JSON files before deploying to Snowflake.

## Prerequisites

- Python 3.10+
- CEIPAL API credentials (email, password, API key)
- Snowflake account (optional for local testing)

## Setup

### 1. Install Dependencies

```bash
cd Portals_Integration
python -m venv .venv
.venv\Scripts\activate          # Windows
source .venv/bin/activate       # Mac/Linux
pip install -r requirements.txt
```

### 2. Configure Credentials

Copy `.env.example` to `.env` and add your credentials:

```bash
cp .env.example .env
```

Edit `.env` and add your CEIPAL credentials:
```
CEIPAL_EMAIL=your.email@company.com
CEIPAL_PASSWORD=your_password
CEIPAL_API_KEY=your_api_key
```

**Optional** - Only needed if extracting expenses or invoices:
```
CEIPAL_FROM_DATE=2025-10-01
CEIPAL_TO_DATE=2025-12-08
```
(Must be within 90 days)

### 3. Test Connection

```bash
python scripts/test_connections.py --ceipal-only
```

**Note**: CEIPAL API is slow - even simple requests take 5-10 minutes. This is normal behavior.

## Quick Start

Run your first extraction (saves to JSON, no Snowflake needed):

```bash
python pipelines/bronze_ingestion.py --resources employees --json-output --sample 5
```

This extracts 5 employee records to `output/data/` (takes ~8 minutes).

Check the results:
```bash
ls output/data/
python scripts/query_employee.py ES-10065
```

## How It Works

The pipeline has three layers, each with its own script:

### 1. Bronze Layer (`pipelines/bronze_ingestion.py`)
- **Purpose**: Extract raw data from CEIPAL API
- **Technology**: Python + requests library
- **Output**: Raw JSON (local files or Snowflake `RAW` schema)
- **Features**: Authentication, pagination, deduplication

### 2. Silver Layer (`pipelines/silver_processing.py`)
- **Purpose**: Clean and validate data
- **Technology**: PySpark
- **Input**: Bronze JSON data
- **Output**: Cleaned tables (local files or Snowflake `SILVER` schema)
- **Transformations**: Parse JSON, clean strings, validate emails/dates, derive fields

### 3. Gold Layer (`pipelines/gold_modeling.py`)
- **Purpose**: Build dimensional model for analytics
- **Technology**: PySpark
- **Input**: Silver tables
- **Output**: Star schema (local files or Snowflake `GOLD` schema)
- **Features**: SCD Type 2, surrogate keys, fact/dimension tables

## Running the Pipeline

### Local Testing (JSON Output)

Extract data to local JSON files:

```bash
# Single resource
python pipelines/bronze_ingestion.py --resources employees --json-output --sample 5

# Multiple resources
python pipelines/bronze_ingestion.py --resources employees,projects,placements --json-output

# All available data
python pipelines/bronze_ingestion.py --all --json-output
```

### Production (Snowflake)

**First time setup:**

1. Add Snowflake credentials to `.env`:
```
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=PORTALS_DW
```

2. Create schemas and tables:
```bash
python scripts/setup_snowflake.py
```

**Run the pipeline:**

```bash
# Full pipeline (Bronze → Silver → Gold)
python pipelines/run_full_pipeline.py --resources employees

# Or run each layer separately
python pipelines/bronze_ingestion.py --resources employees
python pipelines/silver_processing.py --resources employees
python pipelines/gold_modeling.py --resources employees
```

Data lands in:
- `RAW.CEIPAL_EMPLOYEES_BRONZE` - Raw JSON
- `SILVER.CEIPAL_EMPLOYEES` - Cleaned data
- `GOLD.DIM_EMPLOYEES` - Dimension table

### Available Data Sources

**No date range needed:**
- `employees` - Employee records
- `projects` - Project data
- `placements` - Employee assignments
- `clients` - Client companies
- `vendors` - Vendor organizations

**Requires date range in .env:**
- `expenses` - Expense records (needs CEIPAL_FROM_DATE and CEIPAL_TO_DATE)
- `invoices` - Invoice records (needs CEIPAL_FROM_DATE and CEIPAL_TO_DATE)

### Useful Flags

- `--sample N` - Extract only N records (minimum 5)
- `--json-output` - Save to JSON instead of Snowflake
- `--skip-dedup` - Force full reload
- `--debug` - Verbose logging

## Project Structure

```
Portals_Integration/
├── pipelines/                  ← Main scripts (run these)
│   ├── bronze_ingestion.py       Extract (Python)
│   ├── silver_processing.py      Clean (PySpark)
│   ├── gold_modeling.py          Analytics (PySpark)
│   └── run_full_pipeline.py      Run all three
│
├── src/
│   ├── extractors/             ← API client, authentication
│   ├── transformers/
│   │   ├── silver/             ← PySpark cleaning code
│   │   └── gold/               ← PySpark modeling code
│   └── common/                 ← Spark/Snowflake utilities
│
├── scripts/                    ← Utilities
│   ├── test_connections.py
│   ├── setup_snowflake.py
│   └── query_employee.py
│
├── sql/                        ← Snowflake DDL
├── output/                     ← Generated files (gitignored)
└── .env                        ← Your credentials
```

### Where's the PySpark Code?

**Transformations:**
- `src/transformers/silver/` - Cleaning logic for each resource
- `src/transformers/gold/` - Dimensional modeling for each table

**Utilities:**
- `src/common/spark_utils.py` - Spark session management
- `src/common/snowflake_utils.py` - Snowflake read/write

**Entry points:**
- `pipelines/silver_processing.py` - Uses Silver transformers
- `pipelines/gold_modeling.py` - Uses Gold transformers

## Troubleshooting

**Authentication fails**
Check `.env` credentials are correct. Token auto-refreshes, no manual management needed.

**Snowflake connection fails**
Test with `--json-output` first to isolate the issue.

**Takes forever**
That's the CEIPAL API. Use `--sample 5` for testing or query cached data with `scripts/query_employee.py`.

**"Limit should be between 5 to 100"**
CEIPAL requires minimum 5 records. Use `--sample 5` or higher.

**Date range errors**
CEIPAL limits to 90-day windows. Check `CEIPAL_FROM_DATE` and `CEIPAL_TO_DATE` in `.env`.

**Something else**
Check logs in `output/logs/bronze_ingestion_YYYYMMDD.log`

## Quick Reference

```bash
# Test locally
python pipelines/bronze_ingestion.py --resources employees --json-output --sample 5

# Production run
python pipelines/run_full_pipeline.py --resources employees

# Query cached data
python scripts/query_employee.py ES-10065

# Test connections
python scripts/test_connections.py

# Check logs
cat output/logs/bronze_ingestion_*.log
```
