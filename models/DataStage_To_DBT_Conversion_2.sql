{{ config(
    materialized='table',
    tags=['datastage_conversion', 'count_customer_transactions']
) }}

-- DataStage To DBT Conversion
-- Original Job: Count_Customers_Transactions_Job
-- Source: RETAIL_DATA_MART | Type: PxSequentialFile
-- Transformation: Count_Transactions | Type: PxAggregator
-- Target: Count_Customers_Transactions | Type: PxSequentialFile

-- Version 2 Changes:
-- Fixed: Source table reference to use proper Snowflake schema
-- Fixed: Column name casing for Snowflake compatibility
-- Error: Source table 'retail_data_mart' may not exist in raw_data schema
-- Solution: Updated source reference and column names to match Snowflake conventions

-- Job Flow: RETAIL_DATA_MART → Count_Transactions → Count_Customers_Transactions
-- Aggregation: GROUP BY CustomerID, Stockid with COUNT(*) as total_orders_num

WITH source_data AS (
    -- Source: RETAIL_DATA_MART | Field: All fields | Type: Various
    SELECT 
        PRODUCTID,
        PRODUCTNAME,
        CATEGORY,
        SUBCATEGORY,
        SALES,
        QUANTITY,
        CUSTOMERID,  -- Source: RETAIL_DATA_MART | Field: CustomerID | Type: Integer
        CUSTOMERNAME,
        SPENDINGSCORE,
        ANNUALINCOMEK,
        GENDER,
        AGE,
        CUSTOMERTYPE,
        STOCKID,     -- Source: RETAIL_DATA_MART | Field: Stockid | Type: Integer
        NAME,
        RATING,
        LOCATION,
        NOOFEMPLOYEES,
        INVOICEID,
        DESCRIPTION,
        INVOICEDATE,
        PRICE,
        COUNTRY
    FROM {{ ref('raw_retail_data_mart') }}
),

count_transactions AS (
    -- Transformation: Count_Transactions | Type: PxAggregator
    -- Method: sort | Keys: CustomerID, Stockid | Selection: count
    -- Source: Count_Transactions | Field: CustomerID | Type: Integer
    -- Source: Count_Transactions | Field: Stockid | Type: Integer  
    -- Source: Count_Transactions | Field: total_orders_num | Type: Integer
    SELECT 
        CUSTOMERID,
        STOCKID,
        COUNT(*) AS TOTAL_ORDERS_NUM  -- Derivation: RecCount() from DataStage
    FROM source_data
    GROUP BY 
        CUSTOMERID,
        STOCKID
)

-- Final output matching DataStage target schema
-- Target: Count_Customers_Transactions | Output columns: CustomerID, Stockid, total_orders_num
SELECT 
    CUSTOMERID,
    STOCKID, 
    TOTAL_ORDERS_NUM
FROM count_transactions
ORDER BY CUSTOMERID, STOCKID