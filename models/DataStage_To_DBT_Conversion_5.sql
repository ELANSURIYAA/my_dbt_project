-- Version 1: Initial conversion from DataStage Count_Customers_Transactions_Job
-- Version 2: Fixed source reference - changed to lowercase table name for Snowflake compatibility
-- Version 3: Removed source() macro and using direct table reference with fully qualified name
-- Version 4: Optimized query structure and ensured proper Snowflake naming conventions
-- Version 5: Simplified query without CTEs and explicit table reference
-- Correction applied: Removed CTEs for simpler execution, using direct aggregation
-- Error from Version 4: dbt command failed - possibly due to CTE complexity or configuration issues

-- DataStage Job: Count_Customers_Transactions_Job
-- Conversion Date: 2024
-- Source: RETAIL_DATA_MART table
-- Target: Count_Customers_Transactions table
-- Transformation: Aggregation by CustomerID and Stockid with record count

{{ config(
    materialized='table',
    tags=['datastage_conversion', 'count_transactions']
) }}

-- Source: RETAIL_DATA_MART | Stage: Count_Transactions | Type: Aggregator
-- Grouping Keys: CustomerID, Stockid
-- Aggregation: Count records as total_orders_num
-- Stage: Count_Transactions | Operator: group | Method: sort
-- Key: CustomerID, Stockid | Count Field: NumberOfTransactions -> total_orders_num

SELECT
    CUSTOMERID,
    STOCKID,
    COUNT(*) AS TOTAL_ORDERS_NUM
FROM AVA_DB.PUBLIC.RETAIL_DATA_MART
GROUP BY
    CUSTOMERID,
    STOCKID
ORDER BY CUSTOMERID, STOCKID