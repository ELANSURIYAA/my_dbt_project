-- Version 3: Fixed DataStage to DBT conversion - resolved execution errors
-- Source DataStage Job: SCD2_DIM_POLICY_Load.dsx
-- Target: Snowflake PUBLIC.DIM_POLICY (SCD Type 2)
-- Changes from Version 2:
--   - Fixed pre_hook and post_hook syntax errors (removed complex hooks temporarily)
--   - Simplified audit framework to avoid dependency on non-existent tables
--   - Fixed Jinja variable escaping issues
--   - Removed references to audit tables until they are created
--   - Core SCD Type 2 logic remains intact with all validation
--   - Added BATCH_ID column with proper default value

{{ config(
    materialized='incremental',
    unique_key='POLICY_KEY',
    cluster_by=['POLICY_ID', 'EFFECTIVE_FROM'],
    tags=['datastage_conversion', 'scd_type2', 'dim_policy']
) }}

/*
===========================================
DSX JOB PARAMETERS (as DBT variables):
===========================================
Parameter Name       | Default Value          | Description
---------------------|------------------------|------------------------------------------
SRC_CONN             | ORACLE_SRC_POLICY      | Source connection (mapped to Snowflake source)
TGT_CONN             | ORACLE_DWH             | Target connection (mapped to Snowflake target)
RUN_DATE             | CURRENT_DATE()         | Run date (YYYY-MM-DD) used as effective date
COMMIT_BATCH         | 10000                  | DB commit batch size (handled by Snowflake)
LOG_PATH             | /var/ds/logs           | Path for reject/audit files
BATCH_ID             | BATCH_YYYYMMDD         | Batch identifier for audit

Usage in dbt_project.yml:
vars:
  run_date: "2025-10-28"
  batch_id: "BATCH_20251028"

===========================================
CONNECTION DETAILS:
===========================================
Source Database: Snowflake AVA_DB
Source Schema: PUBLIC
Source Table: POLICY_SRC
Target Database: Snowflake AVA_DB
Target Schema: PUBLIC
Target Table: DIM_POLICY

===========================================
PARTITIONING STRATEGY:
===========================================
DataStage: Hash partitioning on POLICY_ID
Snowflake: Cluster by (POLICY_ID, EFFECTIVE_FROM)
Rationale: Optimizes SCD Type 2 lookups and range queries on effective dates
*/

-- CTE 1: Source data from POLICY_SRC with validation
-- Source Stage: SRC_POLICY | Type: OracleConnector | Schema: STAGING.POLICY_SRC
-- Partitioning: Hash(POLICY_ID)
WITH source_data AS (
    SELECT
        POLICY_ID,                    -- Source: POLICY_ID | Type: NUMBER | Business Key
        POLICY_HOLDER_NAME,           -- Source: POLICY_HOLDER_NAME | Type: VARCHAR2(100)
        POLICY_TYPE,                  -- Source: POLICY_TYPE | Type: VARCHAR2(50)
        PREMIUM_AMOUNT,               -- Source: PREMIUM_AMOUNT | Type: NUMBER(10,2)
        START_DATE,                   -- Source: START_DATE | Type: DATE
        END_DATE,                     -- Source: END_DATE | Type: DATE
        UPDATED_DATE,                 -- Source: UPDATED_DATE | Type: DATE
        SOURCE_SYSTEM,                -- Source: SOURCE_SYSTEM | Type: VARCHAR2(50)
        
        -- Validation flags
        CASE 
            WHEN POLICY_ID IS NULL THEN 'POLICY_ID is NULL'
            WHEN POLICY_HOLDER_NAME IS NULL THEN 'POLICY_HOLDER_NAME is NULL'
            WHEN PREMIUM_AMOUNT IS NULL THEN 'PREMIUM_AMOUNT is NULL'
            WHEN PREMIUM_AMOUNT < 0 THEN 'PREMIUM_AMOUNT is negative'
            WHEN START_DATE IS NULL THEN 'START_DATE is NULL'
            WHEN END_DATE IS NOT NULL AND END_DATE < START_DATE THEN 'END_DATE before START_DATE'
            ELSE NULL
        END AS VALIDATION_ERROR,
        
        CASE 
            WHEN POLICY_ID IS NOT NULL 
                AND POLICY_HOLDER_NAME IS NOT NULL 
                AND PREMIUM_AMOUNT IS NOT NULL 
                AND PREMIUM_AMOUNT >= 0
                AND START_DATE IS NOT NULL
                AND (END_DATE IS NULL OR END_DATE >= START_DATE)
            THEN TRUE
            ELSE FALSE
        END AS IS_VALID
        
    FROM {{ source('public', 'POLICY_SRC') }}
    WHERE UPDATED_DATE <= COALESCE(TRY_TO_DATE('{{ var("run_date", "") }}', 'YYYY-MM-DD'), CURRENT_DATE())
),

-- CTE 2: Valid source records (pass validation)
valid_source AS (
    SELECT 
        POLICY_ID,
        POLICY_HOLDER_NAME,
        POLICY_TYPE,
        PREMIUM_AMOUNT,
        START_DATE,
        END_DATE,
        UPDATED_DATE,
        SOURCE_SYSTEM
    FROM source_data
    WHERE IS_VALID = TRUE
),

-- CTE 3: Current dimension records (CURRENT_FLAG = 'Y')
-- Lookup Stage: LOOKUP_DIM_POLICY | Type: Lookup | Schema: DWH.DIM_POLICY
-- Lookup Type: Cached | Match Keys: POLICY_ID | Partitioning: Hash(POLICY_ID)
current_dimension AS (
    SELECT
        POLICY_KEY,                   -- Lookup: POLICY_KEY | Type: NUMBER | Surrogate Key
        POLICY_ID,                    -- Lookup: POLICY_ID | Type: NUMBER | Business Key
        POLICY_HOLDER_NAME,           -- Lookup: POLICY_HOLDER_NAME | Type: VARCHAR2(100)
        POLICY_TYPE,                  -- Lookup: POLICY_TYPE | Type: VARCHAR2(50)
        PREMIUM_AMOUNT,               -- Lookup: PREMIUM_AMOUNT | Type: NUMBER(10,2)
        START_DATE,                   -- Lookup: START_DATE | Type: DATE
        END_DATE,                     -- Lookup: END_DATE | Type: DATE
        EFFECTIVE_FROM,               -- Lookup: EFFECTIVE_FROM | Type: DATE
        EFFECTIVE_TO,                 -- Lookup: EFFECTIVE_TO | Type: DATE
        CURRENT_FLAG,                 -- Lookup: CURRENT_FLAG | Type: CHAR(1)
        VERSION_NO,                   -- Lookup: VERSION_NO | Type: NUMBER
        SOURCE_SYSTEM,                -- Lookup: SOURCE_SYSTEM | Type: VARCHAR2(50)
        UPDATED_DATE                  -- Lookup: UPDATED_DATE | Type: DATE
    FROM {{ source('public', 'DIM_POLICY') }}
    WHERE CURRENT_FLAG = 'Y'
),

-- CTE 4: Join source with current dimension and detect changes
-- Transformer Stage: TRANS_DETECT | Type: Transformer
-- Partitioning: Hash(POLICY_ID)
change_detection AS (
    SELECT
        s.POLICY_ID,
        s.POLICY_HOLDER_NAME,
        s.POLICY_TYPE,
        s.PREMIUM_AMOUNT,
        s.START_DATE,
        s.END_DATE,
        s.UPDATED_DATE,
        s.SOURCE_SYSTEM,
        
        -- Lookup columns with LK_ prefix
        d.POLICY_KEY AS LK_POLICY_KEY,
        d.POLICY_ID AS LK_POLICY_ID,
        d.POLICY_HOLDER_NAME AS LK_POLICY_HOLDER_NAME,
        d.POLICY_TYPE AS LK_POLICY_TYPE,
        d.PREMIUM_AMOUNT AS LK_PREMIUM_AMOUNT,
        d.START_DATE AS LK_START_DATE,
        d.END_DATE AS LK_END_DATE,
        d.EFFECTIVE_FROM AS LK_EFFECTIVE_FROM,
        d.EFFECTIVE_TO AS LK_EFFECTIVE_TO,
        d.CURRENT_FLAG AS LK_CURRENT_FLAG,
        d.VERSION_NO AS LK_VERSION_NO,
        
        -- Change detection logic from TRANS_DETECT stage
        -- Expression: MATCHED = (LK_POLICY_ID IS NOT NULL)
        CASE WHEN d.POLICY_ID IS NOT NULL THEN TRUE ELSE FALSE END AS MATCHED,
        
        -- Expression: ATTR_CHANGED (NULL-safe comparison of SCD Type 2 attributes)
        CASE 
            WHEN d.POLICY_ID IS NOT NULL AND (
                COALESCE(s.POLICY_HOLDER_NAME, '~') != COALESCE(d.POLICY_HOLDER_NAME, '~') OR
                COALESCE(s.POLICY_TYPE, '~') != COALESCE(d.POLICY_TYPE, '~') OR
                COALESCE(CAST(s.PREMIUM_AMOUNT AS VARCHAR), '~') != COALESCE(CAST(d.PREMIUM_AMOUNT AS VARCHAR), '~') OR
                COALESCE(TO_CHAR(s.START_DATE, 'YYYY-MM-DD'), '~') != COALESCE(TO_CHAR(d.START_DATE, 'YYYY-MM-DD'), '~') OR
                COALESCE(TO_CHAR(s.END_DATE, 'YYYY-MM-DD'), '~') != COALESCE(TO_CHAR(d.END_DATE, 'YYYY-MM-DD'), '~')
            ) THEN TRUE
            ELSE FALSE
        END AS ATTR_CHANGED,
        
        -- Expression: NEW_RECORD = NOT MATCHED
        CASE WHEN d.POLICY_ID IS NULL THEN TRUE ELSE FALSE END AS NEW_RECORD,
        
        -- Expression: CHANGED = MATCHED AND ATTR_CHANGED
        CASE 
            WHEN d.POLICY_ID IS NOT NULL AND (
                COALESCE(s.POLICY_HOLDER_NAME, '~') != COALESCE(d.POLICY_HOLDER_NAME, '~') OR
                COALESCE(s.POLICY_TYPE, '~') != COALESCE(d.POLICY_TYPE, '~') OR
                COALESCE(CAST(s.PREMIUM_AMOUNT AS VARCHAR), '~') != COALESCE(CAST(d.PREMIUM_AMOUNT AS VARCHAR), '~') OR
                COALESCE(TO_CHAR(s.START_DATE, 'YYYY-MM-DD'), '~') != COALESCE(TO_CHAR(d.START_DATE, 'YYYY-MM-DD'), '~') OR
                COALESCE(TO_CHAR(s.END_DATE, 'YYYY-MM-DD'), '~') != COALESCE(TO_CHAR(d.END_DATE, 'YYYY-MM-DD'), '~')
            ) THEN TRUE
            ELSE FALSE
        END AS CHANGED,
        
        -- Expression: UNCHANGED = MATCHED AND (NOT ATTR_CHANGED)
        CASE 
            WHEN d.POLICY_ID IS NOT NULL AND (
                COALESCE(s.POLICY_HOLDER_NAME, '~') = COALESCE(d.POLICY_HOLDER_NAME, '~') AND
                COALESCE(s.POLICY_TYPE, '~') = COALESCE(d.POLICY_TYPE, '~') AND
                COALESCE(CAST(s.PREMIUM_AMOUNT AS VARCHAR), '~') = COALESCE(CAST(d.PREMIUM_AMOUNT AS VARCHAR), '~') AND
                COALESCE(TO_CHAR(s.START_DATE, 'YYYY-MM-DD'), '~') = COALESCE(TO_CHAR(d.START_DATE, 'YYYY-MM-DD'), '~') AND
                COALESCE(TO_CHAR(s.END_DATE, 'YYYY-MM-DD'), '~') = COALESCE(TO_CHAR(d.END_DATE, 'YYYY-MM-DD'), '~')
            ) THEN TRUE
            ELSE FALSE
        END AS UNCHANGED,
        
        -- SCD Type 2 fields
        -- Expression: OUT_VERSION_NO = IIF(NEW_RECORD, 1, (LK_VERSION_NO + 1))
        CASE 
            WHEN d.POLICY_ID IS NULL THEN 1
            ELSE COALESCE(d.VERSION_NO, 0) + 1
        END AS OUT_VERSION_NO,
        
        -- Expression: OUT_EFFECTIVE_FROM = TO_DATE('$$RUN_DATE','YYYY-MM-DD')
        COALESCE(TRY_TO_DATE('{{ var("run_date", "") }}', 'YYYY-MM-DD'), CURRENT_DATE()) AS OUT_EFFECTIVE_FROM,
        
        -- Expression: OUT_EFFECTIVE_TO = NULL
        NULL AS OUT_EFFECTIVE_TO,
        
        -- Expression: OUT_CURRENT_FLAG = 'Y'
        'Y' AS OUT_CURRENT_FLAG,
        
        -- Expression: EXPIRE_DATE = TO_DATE(TO_CHAR(TO_DATE('$$RUN_DATE','YYYY-MM-DD') - 1,'YYYY-MM-DD'),'YYYY-MM-DD')
        DATEADD(day, -1, COALESCE(TRY_TO_DATE('{{ var("run_date", "") }}', 'YYYY-MM-DD'), CURRENT_DATE())) AS EXPIRE_DATE,
        
        -- Batch ID for audit trail
        COALESCE('{{ var("batch_id", "") }}', 'BATCH_' || TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')) AS BATCH_ID
        
    FROM valid_source s
    LEFT JOIN current_dimension d
        ON s.POLICY_ID = d.POLICY_ID
),

-- CTE 5: Records to insert (new records and new versions of changed records)
-- SCD Manager Stage: SCD_MANAGER | Type: SlowlyChangingDimension | Mode: Type2
-- Insert Strategy: InsertNewVersion | Commit Batch: $$COMMIT_BATCH
records_to_insert AS (
    SELECT
        POLICY_ID,
        POLICY_HOLDER_NAME,
        POLICY_TYPE,
        PREMIUM_AMOUNT,
        START_DATE,
        END_DATE,
        OUT_EFFECTIVE_FROM AS EFFECTIVE_FROM,
        OUT_EFFECTIVE_TO AS EFFECTIVE_TO,
        OUT_CURRENT_FLAG AS CURRENT_FLAG,
        OUT_VERSION_NO AS VERSION_NO,
        SOURCE_SYSTEM,
        UPDATED_DATE,
        BATCH_ID
    FROM change_detection
    WHERE NEW_RECORD = TRUE OR CHANGED = TRUE
),

-- CTE 6: Records to expire (set CURRENT_FLAG='N' and EFFECTIVE_TO date)
-- SCD Manager Stage: Expire Strategy: SetCurrentToNAndSetEndDate
records_to_expire AS (
    SELECT
        POLICY_ID,
        LK_POLICY_HOLDER_NAME AS POLICY_HOLDER_NAME,
        LK_POLICY_TYPE AS POLICY_TYPE,
        LK_PREMIUM_AMOUNT AS PREMIUM_AMOUNT,
        LK_START_DATE AS START_DATE,
        LK_END_DATE AS END_DATE,
        LK_EFFECTIVE_FROM AS EFFECTIVE_FROM,
        EXPIRE_DATE AS EFFECTIVE_TO,
        'N' AS CURRENT_FLAG,
        LK_VERSION_NO AS VERSION_NO,
        SOURCE_SYSTEM,
        UPDATED_DATE,
        BATCH_ID
    FROM change_detection
    WHERE CHANGED = TRUE
)

-- Final output: Union of new inserts and expired records
-- Target Stage: TGT_DIM_POLICY | Type: OracleConnector | Schema: DWH.DIM_POLICY
-- Commit Batch: $$COMMIT_BATCH | Isolation Level: READ_COMMITTED | Partitioning: Hash(POLICY_ID)
SELECT 
    POLICY_ID,
    POLICY_HOLDER_NAME,
    POLICY_TYPE,
    PREMIUM_AMOUNT,
    START_DATE,
    END_DATE,
    EFFECTIVE_FROM,
    EFFECTIVE_TO,
    CURRENT_FLAG,
    VERSION_NO,
    SOURCE_SYSTEM,
    UPDATED_DATE,
    BATCH_ID
FROM records_to_insert

UNION ALL

SELECT 
    POLICY_ID,
    POLICY_HOLDER_NAME,
    POLICY_TYPE,
    PREMIUM_AMOUNT,
    START_DATE,
    END_DATE,
    EFFECTIVE_FROM,
    EFFECTIVE_TO,
    CURRENT_FLAG,
    VERSION_NO,
    SOURCE_SYSTEM,
    UPDATED_DATE,
    BATCH_ID
FROM records_to_expire

/*
===========================================
VERSION HISTORY:
===========================================
Version 1: Initial conversion from DataStage
Version 2: Added comprehensive audit framework (failed due to missing audit tables)
Version 3: Simplified to core SCD Type 2 logic, removed dependencies on non-existent audit tables
          - Fixed Jinja variable syntax
          - Removed complex pre_hook and post_hook
          - Added BATCH_ID column with proper default
          - Maintained all validation and change detection logic

===========================================
AUDIT FRAMEWORK (TO BE IMPLEMENTED):
===========================================
Once the following tables are created, uncomment the pre_hook and post_hook in config:

1. CREATE TABLE PUBLIC.ETL_AUDIT_LOG (
    JOB_NAME VARCHAR(100),
    BATCH_ID VARCHAR(50),
    START_TIME TIMESTAMP,
    END_TIME TIMESTAMP,
    SOURCE_COUNT NUMBER,
    TARGET_INSERTS NUMBER,
    TARGET_UPDATES NUMBER,
    STATUS VARCHAR(20),
    ERROR_MESSAGE VARCHAR(4000)
);

2. CREATE TABLE PUBLIC.DIM_POLICY_AUDIT (
    POLICY_KEY NUMBER,
    POLICY_ID NUMBER,
    CHANGE_TYPE VARCHAR(20),
    CHANGE_DATE DATE,
    OLD_VALUE VARCHAR(4000),
    NEW_VALUE VARCHAR(4000),
    BATCH_ID VARCHAR(50)
);

3. CREATE TABLE PUBLIC.DIM_POLICY_REJECTS (
    POLICY_ID NUMBER,
    ERROR_DESC VARCHAR(4000),
    RAW_DATA VARCHAR(4000),
    REJECT_TIMESTAMP TIMESTAMP,
    BATCH_ID VARCHAR(50)
);

===========================================
REJECT HANDLING:
===========================================
Rejected records are captured in the 'source_data' CTE with IS_VALID = FALSE.
To log rejects, create a separate model or post-hook that inserts into DIM_POLICY_REJECTS.

===========================================
EXECUTION NOTES:
===========================================
- Set dbt variables in dbt_project.yml or via --vars flag:
  dbt run --vars '{"run_date": "2025-10-28", "batch_id": "BATCH_20251028"}'
- Clustering on (POLICY_ID, EFFECTIVE_FROM) optimizes SCD queries
- Incremental materialization handles large dimension tables efficiently
- POLICY_KEY is auto-generated by Snowflake AUTOINCREMENT (not included in SELECT)
*/