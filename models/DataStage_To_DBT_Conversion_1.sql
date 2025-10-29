-- Version 1: Initial conversion from DataStage Count_Customers_Transactions_Job
-- DataStage Job: Count_Customers_Transactions_Job
-- Conversion Date: 2024
-- Source: RETAIL_DATA_MART table
-- Target: Count_Customers_Transactions table
-- Transformation: Aggregation by CustomerID and Stockid with record count

{{ config(
    materialized='table',
    tags=['datastage_conversion', 'count_transactions'],
    database='AVA_DB',
    schema='PUBLIC'
) }}

-- Source: RETAIL_DATA_MART | Stage: Count_Transactions | Type: Aggregator
-- Grouping Keys: CustomerID, Stockid
-- Aggregation: Count records as total_orders_num

WITH source_data AS (
    -- Source: RETAIL_DATA_MART | Field: CustomerID | Type: Integer
    -- Source: RETAIL_DATA_MART | Field: Stockid | Type: Integer
    SELECT
        CustomerID,
        Stockid
    FROM {{ source('PUBLIC', 'RETAIL_DATA_MART') }}
),

aggregated_data AS (
    -- Stage: Count_Transactions | Operator: group
    -- Method: sort
    -- Key: CustomerID, Stockid
    -- Count Field: NumberOfTransactions -> total_orders_num
    SELECT
        CustomerID,
        Stockid,
        COUNT(*) AS total_orders_num
    FROM source_data
    GROUP BY
        CustomerID,
        Stockid
)

-- Final output matching DataStage target: Count_Customers_Transactions
SELECT
    CustomerID,
    Stockid,
    total_orders_num
FROM aggregated_data
ORDER BY CustomerID, Stockid