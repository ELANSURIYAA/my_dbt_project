-- Version 5: Simplified working version - basic SCD Type 2 implementation
-- Source DataStage Job: SCD2_DIM_POLICY_Load.dsx
-- Target: Snowflake PUBLIC.DIM_POLICY (SCD Type 2)
-- Changes from Version 4:
--   - Simplified to basic SQL without complex Jinja variables
--   - Removed potential parsing issues
--   - Focus on core SCD Type 2 logic
--   - Tested syntax for Snowflake compatibility

{{ config(
    materialized='table',
    tags=['datastage_conversion', 'scd_type2']
) }}

-- Step 1: Get source data with validation
WITH source_data AS (
    SELECT
        POLICY_ID,
        POLICY_HOLDER_NAME,
        POLICY_TYPE,
        PREMIUM_AMOUNT,
        START_DATE,
        END_DATE,
        UPDATED_DATE,
        SOURCE_SYSTEM
    FROM {{ source('public', 'POLICY_SRC') }}
    WHERE POLICY_ID IS NOT NULL
      AND UPDATED_DATE <= CURRENT_DATE()
),

-- Step 2: Get current dimension records
current_dim AS (
    SELECT
        POLICY_KEY,
        POLICY_ID,
        POLICY_HOLDER_NAME,
        POLICY_TYPE,
        PREMIUM_AMOUNT,
        START_DATE,
        END_DATE,
        EFFECTIVE_FROM,
        VERSION_NO
    FROM {{ source('public', 'DIM_POLICY') }}
    WHERE CURRENT_FLAG = 'Y'
),

-- Step 3: Detect changes
changes AS (
    SELECT
        s.*,
        d.POLICY_KEY AS OLD_KEY,
        d.VERSION_NO AS OLD_VERSION,
        d.EFFECTIVE_FROM AS OLD_EFFECTIVE_FROM,
        CASE WHEN d.POLICY_ID IS NULL THEN 1 ELSE 0 END AS IS_NEW,
        CASE 
            WHEN d.POLICY_ID IS NOT NULL AND (
                COALESCE(s.POLICY_HOLDER_NAME, '') != COALESCE(d.POLICY_HOLDER_NAME, '') OR
                COALESCE(s.POLICY_TYPE, '') != COALESCE(d.POLICY_TYPE, '') OR
                COALESCE(s.PREMIUM_AMOUNT, 0) != COALESCE(d.PREMIUM_AMOUNT, 0)
            ) THEN 1 
            ELSE 0 
        END AS IS_CHANGED
    FROM source_data s
    LEFT JOIN current_dim d ON s.POLICY_ID = d.POLICY_ID
),

-- Step 4: New and changed records to insert
inserts AS (
    SELECT
        POLICY_ID,
        POLICY_HOLDER_NAME,
        POLICY_TYPE,
        PREMIUM_AMOUNT,
        START_DATE,
        END_DATE,
        CURRENT_DATE() AS EFFECTIVE_FROM,
        NULL AS EFFECTIVE_TO,
        'Y' AS CURRENT_FLAG,
        CASE WHEN IS_NEW = 1 THEN 1 ELSE OLD_VERSION + 1 END AS VERSION_NO,
        SOURCE_SYSTEM,
        UPDATED_DATE
    FROM changes
    WHERE IS_NEW = 1 OR IS_CHANGED = 1
),

-- Step 5: Expired records
expires AS (
    SELECT
        POLICY_ID,
        POLICY_HOLDER_NAME,
        POLICY_TYPE,
        PREMIUM_AMOUNT,
        START_DATE,
        END_DATE,
        OLD_EFFECTIVE_FROM AS EFFECTIVE_FROM,
        DATEADD(day, -1, CURRENT_DATE()) AS EFFECTIVE_TO,
        'N' AS CURRENT_FLAG,
        OLD_VERSION AS VERSION_NO,
        SOURCE_SYSTEM,
        UPDATED_DATE
    FROM changes
    WHERE IS_CHANGED = 1
)

-- Final: Combine inserts and expires
SELECT * FROM inserts
UNION ALL
SELECT * FROM expires