-- Version 1: Initial conversion from DataStage Count_Customers_Transactions_Job
-- Version 2: Fixed source reference - changed to lowercase table name for Snowflake compatibility
-- Version 3: Removed source() macro and using direct table reference with fully qualified name
-- Version 4: Simplified config - removed alias and using standard DBT pattern
-- Correction applied: Removed alias config to avoid naming conflicts, simplified model structure
-- Error from Version 3: dbt command failed - possible config or naming issue

-- DataStage Job: Count_Customers_Transactions_Job
-- Conversion Date: 2024
-- Source: RETAIL_DATA_MART table
-- Target: Count_Customers_Transactions table
-- Transformation: Aggregation by CustomerID and Stockid with record count

{{ config(
    materialized='table',
    schema='PUBLIC'
) }}

-- Source: RETAIL_DATA_MART | Stage: Count_Transactions | Type: Aggregator
-- Grouping Keys: CustomerID, Stockid
-- Aggregation: Count records as total_orders_num

SELECT
    CUSTOMERID,
    STOCKID,
    COUNT(*) AS TOTAL_ORDERS_NUM
FROM AVA_DB.PUBLIC.RETAIL_DATA_MART
GROUP BY
    CUSTOMERID,
    STOCKID
ORDER BY CUSTOMERID, STOCKID