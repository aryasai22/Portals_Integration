# CEIPAL ETL Pipeline

Enterprise-grade **Extract, Transform, Load (ETL)** pipeline for extracting workforce data from CEIPAL API and loading into Snowflake Data Warehouse using PySpark for distributed processing and Power BI for visualization.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Usage](#usage)
- [Data Pipeline](#data-pipeline)
- [Power BI Integration](#power-bi-integration)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

---

## 🎯 Overview

This ETL pipeline automates the extraction of workforce management data from CEIPAL and provides a complete analytics solution:

```
CEIPAL API → PySpark ETL → Snowflake Data Warehouse → Power BI Dashboards
```

### **Key Entities Extracted:**
- **Employees** - Personnel information, departments, roles
- **Projects** - Project details, timelines, managers
- **Placements** - Job assignments, billing rates, pay rates
- **Clients** - Client information and contracts
- **Vendors** - Vendor relationships
- **Expenses** - Employee expenses and reimbursements
- **Invoices** - Billing and payment records
- **Countries & States** - Geographic reference data

---

## 🏗️ Architecture

### **Data Flow**

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA PIPELINE                             │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  1. EXTRACTION (Source)                                     │
│     ├─ CEIPAL REST API                                      │
│     ├─ Authentication & token management                    │
│     ├─ Rate limiting & retry logic                          │
│     └─ Parallel data enrichment                             │
│                                                              │
│                        ↓                                     │
│                                                              │
│  2. TRANSFORMATION (Processing)                             │
│     ├─ PySpark distributed processing                       │
│     ├─ Schema validation & type conversion                  │
│     ├─ Data quality checks                                  │
│     └─ Metadata enrichment                                  │
│                                                              │
│                        ↓                                     │
│                                                              │
│  3. LOADING (Storage)                                       │
│     Snowflake Data Warehouse                                │
│     ├─ RAW Layer: Source of truth (append-only)            │
│     ├─ STAGING Layer: Temporary processing area            │
│     ├─ WAREHOUSE Layer: Business-ready (SCD Type 2)        │
│     └─ ANALYTICS Layer: Pre-aggregated views               │
│                                                              │
│                        ↓                                     │
│                                                              │
│  4. VISUALIZATION (Business Intelligence)                   │
│     ├─ Power BI dashboards                                  │
│     ├─ Interactive reports & search                         │
│     ├─ Excel exports                                        │
│     └─ Scheduled data refreshes                             │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### **Snowflake Layer Architecture**

| Layer | Purpose | Update Strategy |
|-------|---------|-----------------|
| **RAW** | Immutable source of truth from API | Append-only |
| **STAGING** | Temporary area for incremental loads | Truncate & reload |
| **WAREHOUSE** | Production tables with SCD Type 2 | Merge (upsert) |
| **ANALYTICS** | Business-ready views | Computed on-demand |

---

## ✨ Features

### **ETL Pipeline**
- ✅ Multi-entity support (9+ CEIPAL entities)
- ✅ Distributed processing with PySpark
- ✅ Incremental loads (append/overwrite/merge)
- ✅ Historical tracking (SCD Type 2)
- ✅ Automatic retries & error handling
- ✅ Rate limiting & API throttling
- ✅ Token management & caching
- ✅ Parallel detail enrichment

### **Data Quality**
- ✅ Schema validation on load
- ✅ Type conversion with error handling
- ✅ NULL handling & default values
- ✅ Duplicate detection & deduplication
- ✅ Audit trail & metadata tracking

### **Scalability**
- ✅ Handle millions of records
- ✅ Distributed parallel processing
- ✅ Efficient memory management
- ✅ Optimized Snowflake queries

---

## 📦 Prerequisites

### **Software Requirements**

| Component | Version | Purpose |
|-----------|---------|---------|
| **Python** | 3.8+ | ETL scripting |
| **Java** | 8 or 11 | PySpark runtime |
| **Git** | Latest | Version control |

### **Cloud Services**

| Service | Access Level Required |
|---------|----------------------|
| **CEIPAL API** | API key, email, password |
| **Snowflake** | Database creation, warehouse access |
| **Power BI** | Desktop or Pro license |

### **Check Prerequisites**

```bash
# Check Python version
python --version  # Should be 3.8+

# Check Java version
java -version     # Should be 8 or 11

# Check Git
git --version
```

---

## 🚀 Quick Start

### **1. Clone Repository**

```bash
cd /path/to/projects
git clone <repository-url>
cd Portals_Integration
```

### **2. Create Virtual Environment**

```bash
# Create virtual environment
python -m venv .venv

# Activate (Windows)
.venv\Scripts\activate

# Activate (macOS/Linux)
source .venv/bin/activate
```

### **3. Install Dependencies**

```bash
pip install -r requirements.txt
```

### **4. Configure Environment**

```bash
# Copy example configuration
cp .env.example .env

# Edit .env with your credentials
# - Add CEIPAL API credentials
# - Add Snowflake connection details
```

### **5. Setup Snowflake**

```bash
# Option 1: Using SnowSQL
snowsql -f sql/snowflake_ddl.sql

# Option 2: Snowflake Web UI
# 1. Open Snowflake UI
# 2. Create new worksheet
# 3. Copy contents of sql/snowflake_ddl.sql
# 4. Execute all statements
```

### **6. Run Your First ETL**

```bash
# Test with small dataset (10 employees)
python scripts/dump_ceipal_employees_spark.py \
  --employees \
  --limit 10 \
  --max-pages 1

# Verify in Snowflake
# SELECT COUNT(*) FROM CEIPAL_DW.RAW.EMPLOYEES;
```

📖 **For detailed setup instructions, see [docs/QUICKSTART.md](docs/QUICKSTART.md)**

---

## 📁 Project Structure

```
Portals_Integration/
├── .git/                          # Git version control
├── .venv/                         # Python virtual environment
├── docs/                          # Documentation
│   └── QUICKSTART.md              # Quick setup guide
├── output/                        # ETL outputs (gitignored)
│   └── .gitkeep
├── scripts/                       # ETL scripts
│   ├── ceipal_sync.py             # Token manager & API client
│   └── dump_ceipal_employees_spark.py  # Main ETL script
├── sql/                           # Database scripts
│   └── snowflake_ddl.sql          # Snowflake schema setup
├── .env                           # Configuration (secret, not in git)
├── .env.example                   # Configuration template
├── .gitignore                     # Git ignore rules
├── README.md                      # This file
└── requirements.txt               # Python dependencies
```

---

## ⚙️ Configuration

### **Environment Variables**

Create a `.env` file based on `.env.example`:

```bash
# ============== CEIPAL API ==============
CEIPAL_BASE_URL=https://api.ceipal.com
CEIPAL_EMAIL=your-email@company.com
CEIPAL_PASSWORD=your-password
CEIPAL_API_KEY=your-api-key

# ============== Snowflake ==============
SNOWFLAKE_ACCOUNT=your-account.us-east-1
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=SYSADMIN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=CEIPAL_DW
SNOWFLAKE_SCHEMA=RAW

# ============== ETL Settings ==============
LOAD_STRATEGY=append              # append, overwrite, or merge
DETAIL_RPS=2.0                    # API requests per second
DETAIL_WORKERS=4                  # Parallel workers

# ============== PySpark ==============
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g
```

### **Load Strategies**

| Strategy | Use Case | Speed | Duplicates |
|----------|----------|-------|------------|
| **append** | Initial load, logs | Fast | Possible |
| **overwrite** | Full refresh | Medium | No |
| **merge** | Incremental updates | Slower | No |

---

## 💻 Usage

### **Basic Commands**

```bash
# Load employees
python scripts/dump_ceipal_employees_spark.py --employees

# Load employees with full details
python scripts/dump_ceipal_employees_spark.py --employees --with-details

# Load specific entity
python scripts/dump_ceipal_employees_spark.py --projects --with-details

# Load all entities
python scripts/dump_ceipal_employees_spark.py --all --with-details
```

### **Advanced Options**

```bash
# Incremental load with merge strategy
python scripts/dump_ceipal_employees_spark.py \
  --employees \
  --load-strategy merge \
  --with-details

# Custom rate limiting (8 parallel workers, 5 req/sec)
python scripts/dump_ceipal_employees_spark.py \
  --employees \
  --with-details \
  --workers 8 \
  --rps 5.0

# Limit records for testing
python scripts/dump_ceipal_employees_spark.py \
  --employees \
  --limit 50 \
  --max-pages 2

# Debug mode
python scripts/dump_ceipal_employees_spark.py \
  --employees \
  --debug
```

### **Command-Line Options**

| Option | Description | Default |
|--------|-------------|---------|
| `--employees` | Load employees | Default |
| `--projects` | Load projects | - |
| `--clients` | Load clients | - |
| `--placements` | Load placements | - |
| `--all` | Load all entities | - |
| `--with-details` | Fetch detailed info per record | False |
| `--limit` | Records per page | 100 |
| `--max-pages` | Maximum pages to fetch | None |
| `--workers` | Parallel workers | 4 |
| `--rps` | Rate limit (requests/sec) | 2.0 |
| `--load-strategy` | append/overwrite/merge | append |
| `--debug` | Enable debug logging | False |

---

## 🔄 Data Pipeline

### **1. Extraction Phase**

```python
# The script automatically:
1. Authenticates with CEIPAL API
2. Fetches paginated data (limit=100 per page)
3. Enriches records with detail API calls (if --with-details)
4. Handles rate limiting and retries
5. Caches authentication tokens
```

### **2. Transformation Phase**

```python
# PySpark transformations:
1. Convert Python lists to Spark DataFrames
2. Apply schema validation
3. Add metadata columns (load_timestamp, source_system)
4. Type conversions and NULL handling
5. Partition data for optimal loading
```

### **3. Loading Phase**

```python
# Snowflake loading:
1. Write to STAGING schema (if merge strategy)
2. Load to RAW layer with append/overwrite/merge
3. Update WAREHOUSE layer (SCD Type 2)
4. Refresh ANALYTICS views
```

### **Verify Data in Snowflake**

```sql
-- Check record counts
SELECT COUNT(*) FROM CEIPAL_DW.RAW.EMPLOYEES;

-- View recent records
SELECT * FROM CEIPAL_DW.RAW.EMPLOYEES
ORDER BY load_timestamp DESC
LIMIT 10;

-- Check data quality
SELECT
    COUNT(*) AS total_records,
    SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS missing_email,
    SUM(CASE WHEN department IS NULL THEN 1 ELSE 0 END) AS missing_dept
FROM CEIPAL_DW.RAW.EMPLOYEES
WHERE load_date = CURRENT_DATE();
```

---

## 📊 Power BI Integration

### **Setup Power BI Connection**

1. **Open Power BI Desktop**

2. **Get Data → Snowflake**
   ```
   Server: your-account.snowflakecomputing.com
   Warehouse: COMPUTE_WH
   Database: CEIPAL_DW
   ```

3. **Select Tables**
   ```
   WAREHOUSE.DIM_EMPLOYEES
   WAREHOUSE.DIM_CLIENTS
   WAREHOUSE.FACT_PLACEMENTS
   ANALYTICS.V_REVENUE_BY_CLIENT
   ```

4. **Build Reports**
   - Drag fields to visuals
   - Create filters and slicers
   - Add search boxes
   - Enable Excel export

### **Sample Power BI Queries**

```powerquery
// Active Employees
let
    Source = Snowflake.Databases("your-account.snowflakecomputing.com"),
    Database = Source{[Name="CEIPAL_DW"]}[Data],
    Schema = Database{[Name="WAREHOUSE"]}[Data],
    Table = Schema{[Name="DIM_EMPLOYEES"]}[Data],
    Filtered = Table.SelectRows(Table, each [is_current] = true)
in
    Filtered
```

### **Export to Excel**

Users can:
- Click "Export data" on any visual
- Export entire table to Excel
- Create custom exports with filters

---

## 🐛 Troubleshooting

### **Java Not Found**

```bash
# Error: JAVA_HOME not set

# Solution (Windows)
set JAVA_HOME=C:\Program Files\Java\jdk-11

# Solution (macOS)
export JAVA_HOME=$(/usr/libexec/java_home)

# Solution (Linux)
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
```

### **Snowflake Connection Timeout**

```bash
# Check account format
# Correct: account-name.region.cloud
# Example: xyz12345.us-east-1.aws

# Test connection
python -c "
from snowflake.connector import connect
conn = connect(
    account='your-account',
    user='your-user',
    password='your-pass'
)
print('Connected!')
conn.close()
"
```

### **CEIPAL API Rate Limit**

```bash
# Error: 429 Too Many Requests

# Solution: Reduce rate
python scripts/dump_ceipal_employees_spark.py \
  --employees \
  --rps 0.5 \
  --workers 2
```

### **PySpark Out of Memory**

```bash
# Increase driver memory
export SPARK_DRIVER_MEMORY=8g

# Or in .env file
SPARK_DRIVER_MEMORY=8g
```

### **Common Issues**

| Issue | Solution |
|-------|----------|
| Import error for PySpark | `pip install -r requirements.txt` |
| Snowflake authentication failed | Check credentials in `.env` |
| No data in Snowflake | Verify ETL script completed successfully |
| Duplicate records | Use `--load-strategy merge` |

---

## 📖 Documentation

- **[Quick Start Guide](docs/QUICKSTART.md)** - Get running in 10 minutes
- **[Snowflake Setup](sql/snowflake_ddl.sql)** - Database schema documentation
- **[API Documentation](https://api.ceipal.com/docs)** - CEIPAL API reference

---

## 🤝 Contributing

### **Development Setup**

```bash
# Install dev dependencies
pip install -r requirements.txt

# Run linting
pylint scripts/*.py

# Run tests (if available)
pytest tests/
```

### **Code Style**

- Follow PEP 8 guidelines
- Use type hints where possible
- Add docstrings to functions
- Keep functions focused and testable

---

## 📝 License

Internal use only. Confidential and proprietary.

---

## 📞 Support

For issues or questions:
- Create an issue in this repository
- Contact the Data Engineering team
- Email: data-team@company.com

---

## 🔄 Changelog

### v2.0.0 (2025-10-10)
- ✅ Removed legacy file-based scripts
- ✅ Reorganized project structure
- ✅ Added professional documentation
- ✅ Initialized git repository
- ✅ Enhanced .gitignore for production use
- ✅ Renamed requirements-spark.txt to requirements.txt

### v1.0.0 (2024-XX-XX)
- Initial PySpark + Snowflake implementation
- Multi-entity support
- SCD Type 2 for dimensions
- Analytics views

---

**Built with ❤️ for Enterprise Data Analytics**
