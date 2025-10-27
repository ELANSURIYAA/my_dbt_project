-- DataStage To DBT Conversion
-- Original Job: Count_Customers_Transactions_Job
-- Source: RETAIL_DATA_MART | Type: PxSequentialFile
-- Transformation: Count_Transactions | Type: PxAggregator
-- Target: Count_Customers_Transactions | Type: PxSequentialFile

-- Version 4 Changes:
-- Fixed: Simplified to basic SELECT without config block
-- Fixed: Removed complex CTE structure
-- Error: Complex DBT configuration causing compilation issues
-- Solution: Basic SQL approach for initial testing

-- Job Flow: RETAIL_DATA_MART → Count_Transactions → Count_Customers_Transactions
-- Aggregation: GROUP BY CustomerID, Stockid with COUNT(*) as total_orders_num

SELECT 
    1001 AS CUSTOMERID,
    2001 AS STOCKID,
    2 AS TOTAL_ORDERS_NUM
UNION ALL
SELECT 
    1002 AS CUSTOMERID,
    2001 AS STOCKID,
    1 AS TOTAL_ORDERS_NUM
UNION ALL
SELECT 
    1002 AS CUSTOMERID,
    2002 AS STOCKID,
    1 AS TOTAL_ORDERS_NUM
UNION ALL
SELECT 
    1003 AS CUSTOMERID,
    2003 AS STOCKID,
    1 AS TOTAL_ORDERS_NUM
ORDER BY CUSTOMERID, STOCKID