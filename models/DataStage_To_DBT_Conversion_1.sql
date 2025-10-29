-- Version 1: Initial DataStage to DBT conversion for SCD2_DIM_POLICY
-- Source DataStage Job: SCD2_DIM_POLICY_Load.dsx
-- Target: Snowflake PUBLIC.DIM_POLICY (SCD Type 2)
-- Description: Implements Slowly Changing Dimension Type 2 logic for Policy dimension

{{ config(
    materialized='incremental',
    unique_key='POLICY_KEY',
    tags=['datastage_conversion', 'scd_type2', 'dim_policy']
) }}

-- CTE 1: Source data from POLICY_SRC
-- Source Stage: SRC_POLICY | Type: OracleConnector | Schema: STAGING.POLICY_SRC
WITH source_data AS (
    SELECT
        POLICY_ID,                    -- Source: POLICY_ID | Type: NUMBER
        POLICY_HOLDER_NAME,           -- Source: POLICY_HOLDER_NAME | Type: VARCHAR2(100)
        POLICY_TYPE,                  -- Source: POLICY_TYPE | Type: VARCHAR2(50)
        PREMIUM_AMOUNT,               -- Source: PREMIUM_AMOUNT | Type: NUMBER(10,2)
        START_DATE,                   -- Source: START_DATE | Type: DATE
        END_DATE,                     -- Source: END_DATE | Type: DATE
        UPDATED_DATE,                 -- Source: UPDATED_DATE | Type: DATE
        SOURCE_SYSTEM                 -- Source: SOURCE_SYSTEM | Type: VARCHAR2(50)
    FROM {{ source('public', 'POLICY_SRC') }}
    WHERE UPDATED_DATE <= CURRENT_DATE()
),

-- CTE 2: Current dimension records (CURRENT_FLAG = 'Y')
-- Lookup Stage: LOOKUP_DIM_POLICY | Type: Lookup | Schema: DWH.DIM_POLICY
current_dimension AS (
    SELECT
        POLICY_KEY,                   -- Lookup: POLICY_KEY | Type: NUMBER
        POLICY_ID,                    -- Lookup: POLICY_ID | Type: NUMBER
        POLICY_HOLDER_NAME,           -- Lookup: POLICY_HOLDER_NAME | Type: VARCHAR2(100)
        POLICY_TYPE,                  -- Lookup: POLICY_TYPE | Type: VARCHAR2(50)
        PREMIUM_AMOUNT,               -- Lookup: PREMIUM_AMOUNT | Type: NUMBER(10,2)
        START_DATE,                   -- Lookup: START_DATE | Type: DATE
        END_DATE,                     -- Lookup: END_DATE | Type: DATE
        EFFECTIVE_FROM,               -- Lookup: EFFECTIVE_FROM | Type: DATE
        EFFECTIVE_TO,                 -- Lookup: EFFECTIVE_TO | Type: DATE
        CURRENT_FLAG,                 -- Lookup: CURRENT_FLAG | Type: CHAR(1)
        VERSION_NO                    -- Lookup: VERSION_NO | Type: NUMBER
    FROM {{ source('public', 'DIM_POLICY') }}
    WHERE CURRENT_FLAG = 'Y'
),

-- CTE 3: Join source with current dimension and detect changes
-- Transformer Stage: TRANS_DETECT | Type: Transformer
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
        d.VERSION_NO AS LK_VERSION_NO,
        
        -- Change detection logic from TRANS_DETECT stage
        CASE WHEN d.POLICY_ID IS NOT NULL THEN TRUE ELSE FALSE END AS MATCHED,
        
        -- Attribute comparison (NULL-safe)
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
        
        -- Record classification
        CASE WHEN d.POLICY_ID IS NULL THEN TRUE ELSE FALSE END AS NEW_RECORD,
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
        
        -- SCD Type 2 fields
        CASE 
            WHEN d.POLICY_ID IS NULL THEN 1
            ELSE COALESCE(d.VERSION_NO, 0) + 1
        END AS OUT_VERSION_NO,
        
        CURRENT_DATE() AS OUT_EFFECTIVE_FROM,
        NULL AS OUT_EFFECTIVE_TO,
        'Y' AS OUT_CURRENT_FLAG,
        DATEADD(day, -1, CURRENT_DATE()) AS EXPIRE_DATE
        
    FROM source_data s
    LEFT JOIN current_dimension d
        ON s.POLICY_ID = d.POLICY_ID
    WHERE s.POLICY_ID IS NOT NULL  -- Validation: POLICY_ID not null
),

-- CTE 4: Records to insert (new records and new versions of changed records)
records_to_insert AS (
    SELECT
        NULL AS POLICY_KEY,  -- Will be auto-generated by Snowflake AUTOINCREMENT
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
        UPDATED_DATE
    FROM change_detection
    WHERE NEW_RECORD = TRUE OR CHANGED = TRUE
),

-- CTE 5: Records to expire (set CURRENT_FLAG='N' and EFFECTIVE_TO date)
records_to_expire AS (
    SELECT
        LK_POLICY_KEY AS POLICY_KEY,
        POLICY_ID,
        LK_POLICY_HOLDER_NAME AS POLICY_HOLDER_NAME,
        LK_POLICY_TYPE AS POLICY_TYPE,
        LK_PREMIUM_AMOUNT AS PREMIUM_AMOUNT,
        LK_START_DATE AS START_DATE,
        LK_END_DATE AS END_DATE,
        OUT_EFFECTIVE_FROM AS EFFECTIVE_FROM,
        EXPIRE_DATE AS EFFECTIVE_TO,
        'N' AS CURRENT_FLAG,
        LK_VERSION_NO AS VERSION_NO,
        SOURCE_SYSTEM,
        UPDATED_DATE
    FROM change_detection
    WHERE CHANGED = TRUE
)

-- Final output: Union of new inserts and expired records
SELECT * FROM records_to_insert
UNION ALL
SELECT * FROM records_to_expire