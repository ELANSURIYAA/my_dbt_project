-- Version 1: Initial conversion from DataStage Count_Customers_Transactions_Job
-- Version 2: Fixed source reference - changed to lowercase table name for Snowflake compatibility
-- Version 3: Removed source() macro and using direct table reference with fully qualified name
-- Correction applied: Changed from source() macro to direct table reference AVA_DB.PUBLIC.RETAIL_DATA_MART
-- Error from Version 2: dbt command failed - source reference may not be properly configured

-- DataStage Job: Count_Customers_Transactions_Job
-- Conversion Date: 2024
-- Source: RETAIL_DATA_MART table
-- Target: Count_Customers_Transactions table
-- Transformation: Aggregation by CustomerID and Stockid with record count

{{ config(
    materialized='table',
    tags=['datastage_conversion', 'count_transactions'],
    database='AVA_DB',
    schema='PUBLIC',
    alias='count_customers_transactions'
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