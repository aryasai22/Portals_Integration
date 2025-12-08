-- =====================================================
-- Snowflake Schema Setup for CEIPAL Integration
-- =====================================================
-- Creates all required schemas for medallion architecture

USE DATABASE PORTALS_DW;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Bronze layer - Raw JSON data from CEIPAL API';

CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Staging area for MERGE operations';

CREATE SCHEMA IF NOT EXISTS SILVER
    COMMENT = 'Silver layer - Cleaned, typed, normalized data';

CREATE SCHEMA IF NOT EXISTS GOLD
    COMMENT = 'Gold layer - Dimensional star schema for analytics';

-- Verify schemas
SHOW SCHEMAS IN DATABASE PORTALS_DW;
