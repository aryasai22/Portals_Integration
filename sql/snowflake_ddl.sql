-- ============================================================
-- CEIPAL Data Warehouse - Snowflake DDL
-- ============================================================
-- This script creates the database, schemas, and tables for the CEIPAL ETL pipeline.
-- Execute as ACCOUNTADMIN or user with CREATE DATABASE privileges.
-- ============================================================

-- ============================================================
-- 1. Database and Schema Setup
-- ============================================================

-- Create database
CREATE DATABASE IF NOT EXISTS CEIPAL_DW
    COMMENT = 'CEIPAL workforce management data warehouse';

USE DATABASE CEIPAL_DW;

-- Create schemas for different stages of data pipeline
CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Raw data from CEIPAL API - append-only';

CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Temporary staging area for incremental loads';

CREATE SCHEMA IF NOT EXISTS WAREHOUSE
    COMMENT = 'Production data warehouse tables with SCD';

CREATE SCHEMA IF NOT EXISTS ANALYTICS
    COMMENT = 'Analytics-ready views and aggregations';


-- ============================================================
-- 2. RAW Layer Tables (Direct API Dumps)
-- ============================================================

-- Raw Employees Table
CREATE OR REPLACE TABLE RAW.EMPLOYEES (
    id VARCHAR(255),
    employee_id VARCHAR(255),
    employee_number VARCHAR(100),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(500),
    phone VARCHAR(50),
    status VARCHAR(50),
    department VARCHAR(255),
    job_title VARCHAR(255),
    hire_date VARCHAR(50),
    termination_date VARCHAR(50),
    manager_id VARCHAR(255),
    location VARCHAR(500),
    detail VARIANT,  -- JSON column for flexible nested data
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system VARCHAR(50) DEFAULT 'CEIPAL_API',
    load_date DATE,
    _metadata_file_path VARCHAR(1000)
)
COMMENT = 'Raw employee data from CEIPAL API';

-- Raw Projects Table
CREATE OR REPLACE TABLE RAW.PROJECTS (
    id VARCHAR(255),
    project_id VARCHAR(255),
    project_code VARCHAR(100),
    project_name VARCHAR(500),
    client_id VARCHAR(255),
    client_name VARCHAR(500),
    start_date VARCHAR(50),
    end_date VARCHAR(50),
    status VARCHAR(50),
    project_manager VARCHAR(255),
    detail VARIANT,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system VARCHAR(50) DEFAULT 'CEIPAL_API',
    load_date DATE
)
COMMENT = 'Raw project data from CEIPAL API';

-- Raw Placements Table
CREATE OR REPLACE TABLE RAW.PLACEMENTS (
    id VARCHAR(255),
    placement_id VARCHAR(255),
    job_code VARCHAR(100),
    employee_id VARCHAR(255),
    client_id VARCHAR(255),
    project_id VARCHAR(255),
    start_date VARCHAR(50),
    end_date VARCHAR(50),
    status VARCHAR(50),
    billing_rate NUMBER(18,2),
    pay_rate NUMBER(18,2),
    detail VARIANT,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system VARCHAR(50) DEFAULT 'CEIPAL_API',
    load_date DATE
)
COMMENT = 'Raw placement/assignment data from CEIPAL API';

-- Raw Clients Table
CREATE OR REPLACE TABLE RAW.CLIENTS (
    id VARCHAR(255),
    client_id VARCHAR(255),
    client_name VARCHAR(500),
    client_code VARCHAR(100),
    status VARCHAR(50),
    address VARCHAR(1000),
    city VARCHAR(255),
    state VARCHAR(255),
    country VARCHAR(100),
    contact_name VARCHAR(255),
    contact_email VARCHAR(500),
    contact_phone VARCHAR(50),
    detail VARIANT,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system VARCHAR(50) DEFAULT 'CEIPAL_API',
    load_date DATE
)
COMMENT = 'Raw client data from CEIPAL API';

-- Raw Vendors Table
CREATE OR REPLACE TABLE RAW.VENDORS (
    id VARCHAR(255),
    vendor_id VARCHAR(255),
    vendor_name VARCHAR(500),
    vendor_code VARCHAR(100),
    status VARCHAR(50),
    address VARCHAR(1000),
    city VARCHAR(255),
    state VARCHAR(255),
    country VARCHAR(100),
    contact_name VARCHAR(255),
    contact_email VARCHAR(500),
    contact_phone VARCHAR(50),
    detail VARIANT,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system VARCHAR(50) DEFAULT 'CEIPAL_API',
    load_date DATE
)
COMMENT = 'Raw vendor data from CEIPAL API';

-- Raw Expenses Table
CREATE OR REPLACE TABLE RAW.EXPENSES (
    id VARCHAR(255),
    expense_id VARCHAR(255),
    employee_id VARCHAR(255),
    placement_id VARCHAR(255),
    expense_date VARCHAR(50),
    expense_type VARCHAR(100),
    amount NUMBER(18,2),
    currency VARCHAR(10),
    status VARCHAR(50),
    description VARCHAR(2000),
    detail VARIANT,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system VARCHAR(50) DEFAULT 'CEIPAL_API',
    load_date DATE
)
COMMENT = 'Raw expense data from CEIPAL API';

-- Raw Invoices Table
CREATE OR REPLACE TABLE RAW.INVOICES (
    id VARCHAR(255),
    invoice_id VARCHAR(255),
    invoice_number VARCHAR(100),
    client_id VARCHAR(255),
    client_name VARCHAR(500),
    invoice_date VARCHAR(50),
    due_date VARCHAR(50),
    total_amount NUMBER(18,2),
    currency VARCHAR(10),
    status VARCHAR(50),
    detail VARIANT,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system VARCHAR(50) DEFAULT 'CEIPAL_API',
    load_date DATE
)
COMMENT = 'Raw invoice data from CEIPAL API';


-- ============================================================
-- 3. STAGING Layer Tables (for MERGE operations)
-- ============================================================

CREATE OR REPLACE TABLE STAGING.STG_EMPLOYEES LIKE RAW.EMPLOYEES;
CREATE OR REPLACE TABLE STAGING.STG_PROJECTS LIKE RAW.PROJECTS;
CREATE OR REPLACE TABLE STAGING.STG_PLACEMENTS LIKE RAW.PLACEMENTS;
CREATE OR REPLACE TABLE STAGING.STG_CLIENTS LIKE RAW.CLIENTS;
CREATE OR REPLACE TABLE STAGING.STG_VENDORS LIKE RAW.VENDORS;
CREATE OR REPLACE TABLE STAGING.STG_EXPENSES LIKE RAW.EXPENSES;
CREATE OR REPLACE TABLE STAGING.STG_INVOICES LIKE RAW.INVOICES;


-- ============================================================
-- 4. WAREHOUSE Layer Tables (SCD Type 2 for history tracking)
-- ============================================================

-- Employees Dimension (SCD Type 2)
CREATE OR REPLACE TABLE WAREHOUSE.DIM_EMPLOYEES (
    employee_key NUMBER AUTOINCREMENT PRIMARY KEY,  -- Surrogate key
    id VARCHAR(255),
    employee_id VARCHAR(255),
    employee_number VARCHAR(100),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(500),
    phone VARCHAR(50),
    status VARCHAR(50),
    department VARCHAR(255),
    job_title VARCHAR(255),
    hire_date DATE,
    termination_date DATE,
    manager_id VARCHAR(255),
    location VARCHAR(500),

    -- SCD Type 2 columns
    valid_from TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    valid_to TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('9999-12-31 23:59:59'),
    is_current BOOLEAN DEFAULT TRUE,

    -- Audit columns
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    row_hash VARCHAR(64),  -- MD5 hash for change detection

    detail VARIANT
)
COMMENT = 'Employee dimension with SCD Type 2 history';

-- Projects Dimension
CREATE OR REPLACE TABLE WAREHOUSE.DIM_PROJECTS (
    project_key NUMBER AUTOINCREMENT PRIMARY KEY,
    id VARCHAR(255),
    project_id VARCHAR(255),
    project_code VARCHAR(100),
    project_name VARCHAR(500),
    client_id VARCHAR(255),
    client_name VARCHAR(500),
    start_date DATE,
    end_date DATE,
    status VARCHAR(50),
    project_manager VARCHAR(255),

    valid_from TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    valid_to TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('9999-12-31 23:59:59'),
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    detail VARIANT
)
COMMENT = 'Project dimension with SCD Type 2 history';

-- Clients Dimension
CREATE OR REPLACE TABLE WAREHOUSE.DIM_CLIENTS (
    client_key NUMBER AUTOINCREMENT PRIMARY KEY,
    id VARCHAR(255),
    client_id VARCHAR(255),
    client_name VARCHAR(500),
    client_code VARCHAR(100),
    status VARCHAR(50),
    address VARCHAR(1000),
    city VARCHAR(255),
    state VARCHAR(255),
    country VARCHAR(100),

    valid_from TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    valid_to TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('9999-12-31 23:59:59'),
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    detail VARIANT
)
COMMENT = 'Client dimension with SCD Type 2 history';

-- Placements Fact Table
CREATE OR REPLACE TABLE WAREHOUSE.FACT_PLACEMENTS (
    placement_key NUMBER AUTOINCREMENT PRIMARY KEY,
    id VARCHAR(255),
    placement_id VARCHAR(255),
    job_code VARCHAR(100),

    -- Foreign keys
    employee_key NUMBER,
    client_key NUMBER,
    project_key NUMBER,

    start_date DATE,
    end_date DATE,
    status VARCHAR(50),
    billing_rate NUMBER(18,2),
    pay_rate NUMBER(18,2),
    margin NUMBER(18,2),  -- Calculated: billing_rate - pay_rate

    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    detail VARIANT
)
COMMENT = 'Placement fact table';

-- Expenses Fact Table
CREATE OR REPLACE TABLE WAREHOUSE.FACT_EXPENSES (
    expense_key NUMBER AUTOINCREMENT PRIMARY KEY,
    id VARCHAR(255),
    expense_id VARCHAR(255),
    employee_key NUMBER,
    placement_key NUMBER,
    expense_date DATE,
    expense_type VARCHAR(100),
    amount NUMBER(18,2),
    currency VARCHAR(10),
    status VARCHAR(50),

    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    detail VARIANT
)
COMMENT = 'Expense transactions fact table';

-- Invoices Fact Table
CREATE OR REPLACE TABLE WAREHOUSE.FACT_INVOICES (
    invoice_key NUMBER AUTOINCREMENT PRIMARY KEY,
    id VARCHAR(255),
    invoice_id VARCHAR(255),
    invoice_number VARCHAR(100),
    client_key NUMBER,
    invoice_date DATE,
    due_date DATE,
    total_amount NUMBER(18,2),
    currency VARCHAR(10),
    status VARCHAR(50),

    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    detail VARIANT
)
COMMENT = 'Invoice fact table';


-- ============================================================
-- 5. ANALYTICS Layer Views
-- ============================================================

-- Active Employees
CREATE OR REPLACE VIEW ANALYTICS.V_ACTIVE_EMPLOYEES AS
SELECT
    employee_key,
    employee_id,
    employee_number,
    CONCAT(first_name, ' ', last_name) AS full_name,
    email,
    department,
    job_title,
    hire_date,
    manager_id,
    location,
    DATEDIFF(day, hire_date, CURRENT_DATE()) AS tenure_days
FROM WAREHOUSE.DIM_EMPLOYEES
WHERE is_current = TRUE
  AND status = 'Active'
COMMENT = 'View of currently active employees';

-- Employee Placement History
CREATE OR REPLACE VIEW ANALYTICS.V_EMPLOYEE_PLACEMENT_HISTORY AS
SELECT
    e.employee_number,
    e.first_name,
    e.last_name,
    p.placement_id,
    p.job_code,
    c.client_name,
    proj.project_name,
    p.start_date,
    p.end_date,
    p.billing_rate,
    p.pay_rate,
    p.margin,
    p.status AS placement_status
FROM WAREHOUSE.FACT_PLACEMENTS p
LEFT JOIN WAREHOUSE.DIM_EMPLOYEES e ON p.employee_key = e.employee_key
LEFT JOIN WAREHOUSE.DIM_CLIENTS c ON p.client_key = c.client_key
LEFT JOIN WAREHOUSE.DIM_PROJECTS proj ON p.project_key = proj.project_key
WHERE e.is_current = TRUE
ORDER BY p.start_date DESC
COMMENT = 'Complete placement history with employee and client details';

-- Revenue by Client
CREATE OR REPLACE VIEW ANALYTICS.V_REVENUE_BY_CLIENT AS
SELECT
    c.client_name,
    c.client_code,
    COUNT(DISTINCT p.placement_id) AS total_placements,
    COUNT(DISTINCT p.employee_key) AS total_employees,
    SUM(p.billing_rate) AS total_billing_rate,
    AVG(p.billing_rate) AS avg_billing_rate,
    SUM(p.margin) AS total_margin
FROM WAREHOUSE.FACT_PLACEMENTS p
JOIN WAREHOUSE.DIM_CLIENTS c ON p.client_key = c.client_key
WHERE p.status = 'Active'
  AND c.is_current = TRUE
GROUP BY c.client_name, c.client_code
ORDER BY total_margin DESC
COMMENT = 'Revenue metrics aggregated by client';

-- Monthly Expense Summary
CREATE OR REPLACE VIEW ANALYTICS.V_MONTHLY_EXPENSES AS
SELECT
    DATE_TRUNC('month', expense_date) AS expense_month,
    expense_type,
    COUNT(*) AS expense_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount,
    currency
FROM WAREHOUSE.FACT_EXPENSES
WHERE status = 'Approved'
GROUP BY expense_month, expense_type, currency
ORDER BY expense_month DESC, total_amount DESC
COMMENT = 'Monthly expense summary by type';


-- ============================================================
-- 6. Indexes and Clustering
-- ============================================================

-- Cluster keys for better query performance
ALTER TABLE RAW.EMPLOYEES CLUSTER BY (load_date, employee_number);
ALTER TABLE RAW.PROJECTS CLUSTER BY (load_date, project_code);
ALTER TABLE RAW.PLACEMENTS CLUSTER BY (load_date, status);

ALTER TABLE WAREHOUSE.DIM_EMPLOYEES CLUSTER BY (is_current, department);
ALTER TABLE WAREHOUSE.FACT_PLACEMENTS CLUSTER BY (start_date, status);


-- ============================================================
-- 7. Stored Procedures for ETL
-- ============================================================

-- Procedure to refresh employee dimension with SCD Type 2
CREATE OR REPLACE PROCEDURE WAREHOUSE.SP_REFRESH_DIM_EMPLOYEES()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Expire old records
    UPDATE WAREHOUSE.DIM_EMPLOYEES dst
    SET
        valid_to = CURRENT_TIMESTAMP(),
        is_current = FALSE,
        updated_at = CURRENT_TIMESTAMP()
    WHERE dst.is_current = TRUE
      AND EXISTS (
          SELECT 1 FROM STAGING.STG_EMPLOYEES src
          WHERE src.id = dst.id
            AND MD5(CONCAT_WS('|',
                src.first_name, src.last_name, src.email, src.department,
                src.job_title, src.status
            )) != dst.row_hash
      );

    -- Insert new/changed records
    INSERT INTO WAREHOUSE.DIM_EMPLOYEES (
        id, employee_id, employee_number, first_name, last_name, email,
        phone, status, department, job_title, hire_date, termination_date,
        manager_id, location, detail, row_hash, valid_from, is_current
    )
    SELECT
        src.id,
        src.employee_id,
        src.employee_number,
        src.first_name,
        src.last_name,
        src.email,
        src.phone,
        src.status,
        src.department,
        src.job_title,
        TRY_TO_DATE(src.hire_date),
        TRY_TO_DATE(src.termination_date),
        src.manager_id,
        src.location,
        src.detail,
        MD5(CONCAT_WS('|',
            src.first_name, src.last_name, src.email, src.department,
            src.job_title, src.status
        )),
        CURRENT_TIMESTAMP(),
        TRUE
    FROM STAGING.STG_EMPLOYEES src
    LEFT JOIN WAREHOUSE.DIM_EMPLOYEES dst
        ON src.id = dst.id AND dst.is_current = TRUE
    WHERE dst.id IS NULL  -- New records
       OR MD5(CONCAT_WS('|',
            src.first_name, src.last_name, src.email, src.department,
            src.job_title, src.status
          )) != dst.row_hash;  -- Changed records

    RETURN 'DIM_EMPLOYEES refreshed successfully';
END;
$$;


-- ============================================================
-- 8. Grant Permissions
-- ============================================================

-- Grant usage on database and schemas
GRANT USAGE ON DATABASE CEIPAL_DW TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA CEIPAL_DW.RAW TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA CEIPAL_DW.STAGING TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA CEIPAL_DW.WAREHOUSE TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA CEIPAL_DW.ANALYTICS TO ROLE SYSADMIN;

-- Grant read/write on tables
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA CEIPAL_DW.RAW TO ROLE SYSADMIN;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA CEIPAL_DW.STAGING TO ROLE SYSADMIN;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA CEIPAL_DW.WAREHOUSE TO ROLE SYSADMIN;
GRANT SELECT ON ALL VIEWS IN SCHEMA CEIPAL_DW.ANALYTICS TO ROLE SYSADMIN;

-- Grant for future objects
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA CEIPAL_DW.RAW TO ROLE SYSADMIN;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA CEIPAL_DW.STAGING TO ROLE SYSADMIN;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA CEIPAL_DW.WAREHOUSE TO ROLE SYSADMIN;


-- ============================================================
-- 9. Example Queries
-- ============================================================

/*
-- Check recent loads
SELECT load_date, COUNT(*) as record_count
FROM RAW.EMPLOYEES
GROUP BY load_date
ORDER BY load_date DESC;

-- View active placements with full details
SELECT * FROM ANALYTICS.V_EMPLOYEE_PLACEMENT_HISTORY
WHERE placement_status = 'Active';

-- Revenue by client
SELECT * FROM ANALYTICS.V_REVENUE_BY_CLIENT
LIMIT 10;

-- Audit employee changes
SELECT
    employee_number,
    first_name,
    last_name,
    department,
    job_title,
    valid_from,
    valid_to,
    is_current
FROM WAREHOUSE.DIM_EMPLOYEES
WHERE employee_number = 'EMP001'
ORDER BY valid_from DESC;
*/
