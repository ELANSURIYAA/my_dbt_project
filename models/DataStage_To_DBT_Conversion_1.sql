-- Version 1: Initial DataStage to DBT conversion
-- DataStage Job: Count_Customers_Transactions_Job
-- Source: RETAIL_DATA_MART | Target: Count_Customers_Transactions
-- Transformation: Aggregator (Group by CustomerID, Stockid and count records)

{{ config(
    materialized='table',
    tags=['datastage_conversion', 'count_transactions'],
    schema='PUBLIC',
    database='AVA_DB'
) }}

-- Source: RETAIL_DATA_MART | Stage: Sequential File Input
-- Aggregator Stage: Count_Transactions
-- Method: Sort | Keys: CustomerID, Stockid | Count Field: NumberOfTransactions

WITH source_data AS (
    -- Source: RETAIL_DATA_MART | Field: CustomerID | Type: Integer
    -- Source: RETAIL_DATA_MART | Field: Stockid | Type: Integer
    SELECT
        CustomerID,
        Stockid
    FROM {{ source('PUBLIC', 'RETAIL_DATA_MART') }}
),

aggregated_data AS (
    -- Transformation: Count_Transactions (Aggregator)
    -- Derivation: total_orders_num = RecCount() grouped by CustomerID, Stockid
    SELECT
        CustomerID,
        Stockid,
        COUNT(*) AS total_orders_num
    FROM source_data
    GROUP BY
        CustomerID,
        Stockid
)

-- Final Output: Count_Customers_Transactions
-- Target: Sequential File | File: Count_Customers_Transactions.txt
SELECT
    CustomerID,
    Stockid,
    total_orders_num
FROM aggregated_data
ORDER BY CustomerID, Stockid