-- Version 1: Initial conversion from DataStage Count_Customers_Transactions_Job
-- Version 2: Fixed source reference - changed to lowercase table name for Snowflake compatibility
-- Correction applied: Source table name changed from RETAIL_DATA_MART to retail_data_mart
-- Error from Version 1: dbt command failed - likely due to case sensitivity in source reference

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
        customerid,
        stockid
    FROM {{ source('PUBLIC', 'retail_data_mart') }}
),

aggregated_data AS (
    -- Stage: Count_Transactions | Operator: group
    -- Method: sort
    -- Key: CustomerID, Stockid
    -- Count Field: NumberOfTransactions -> total_orders_num
    SELECT
        customerid,
        stockid,
        COUNT(*) AS total_orders_num
    FROM source_data
    GROUP BY
        customerid,
        stockid
)

-- Final output matching DataStage target: Count_Customers_Transactions
SELECT
    customerid,
    stockid,
    total_orders_num
FROM aggregated_data
ORDER BY customerid, stockid