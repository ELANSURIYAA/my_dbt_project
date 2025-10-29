-- Version 1: Initial conversion from DataStage Count_Customers_Transactions_Job
-- Version 2: Fixed source reference - changed to lowercase table name for Snowflake compatibility
-- Version 3: Removed source() macro and using direct table reference with fully qualified name
-- Version 4: Optimized query structure and ensured proper Snowflake naming conventions
-- Correction applied: Simplified configuration and ensured consistent uppercase naming for Snowflake
-- Note: User requested changes but did not specify details - applying general optimizations

-- DataStage Job: Count_Customers_Transactions_Job
-- Conversion Date: 2024
-- Source: RETAIL_DATA_MART table
-- Target: Count_Customers_Transactions table
-- Transformation: Aggregation by CustomerID and Stockid with record count

{{ config(
    materialized='table',
    tags=['datastage_conversion', 'count_transactions'],
    schema='PUBLIC'
) }}

-- Source: RETAIL_DATA_MART | Stage: Count_Transactions | Type: Aggregator
-- Grouping Keys: CustomerID, Stockid
-- Aggregation: Count records as total_orders_num

WITH source_data AS (
    -- Source: RETAIL_DATA_MART | Field: CustomerID | Type: Integer
    -- Source: RETAIL_DATA_MART | Field: Stockid | Type: Integer
    SELECT
        CUSTOMERID,
        STOCKID
    FROM AVA_DB.PUBLIC.RETAIL_DATA_MART
),

aggregated_data AS (
    -- Stage: Count_Transactions | Operator: group
    -- Method: sort
    -- Key: CustomerID, Stockid
    -- Count Field: NumberOfTransactions -> total_orders_num
    SELECT
        CUSTOMERID,
        STOCKID,
        COUNT(*) AS TOTAL_ORDERS_NUM
    FROM source_data
    GROUP BY
        CUSTOMERID,
        STOCKID
)

-- Final output matching DataStage target: Count_Customers_Transactions
SELECT
    CUSTOMERID,
    STOCKID,
    TOTAL_ORDERS_NUM
FROM aggregated_data
ORDER BY CUSTOMERID, STOCKID