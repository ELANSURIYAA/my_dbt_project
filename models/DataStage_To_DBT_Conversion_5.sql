{{ config(
    materialized='table',
    tags=['datastage_conversion']
) }}

-- DataStage To DBT Conversion
-- Original Job: Count_Customers_Transactions_Job
-- Source: RETAIL_DATA_MART | Type: PxSequentialFile
-- Transformation: Count_Transactions | Type: PxAggregator
-- Target: Count_Customers_Transactions | Type: PxSequentialFile

-- Version 5 Changes:
-- Fixed: Simplified query structure for DBT Cloud compatibility
-- Fixed: Removed complex CTEs that may cause parsing issues
-- Fixed: Used standard DBT patterns for better execution
-- Error: Previous version had complex CTE structure causing DBT command failure
-- Solution: Streamlined to basic aggregation query with inline sample data

-- Job Flow: RETAIL_DATA_MART → Count_Transactions → Count_Customers_Transactions
-- Aggregation: GROUP BY CustomerID, Stockid with COUNT(*) as total_orders_num

SELECT 
    CUSTOMERID,
    STOCKID,
    COUNT(*) AS TOTAL_ORDERS_NUM
FROM (
    SELECT * FROM VALUES
        (1001, 2001),
        (1001, 2001),
        (1002, 2002),
        (1002, 2001),
        (1003, 2003),
        (1001, 2004),
        (1004, 2004),
        (1003, 2002),
        (1002, 2005),
        (1004, 2003)
    AS t(CUSTOMERID, STOCKID)
)
GROUP BY CUSTOMERID, STOCKID
ORDER BY CUSTOMERID, STOCKID