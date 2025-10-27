{{ config(
    materialized='table',
    tags=['datastage_conversion', 'count_customer_transactions']
) }}

-- DataStage To DBT Conversion
-- Original Job: Count_Customers_Transactions_Job
-- Source: RETAIL_DATA_MART | Type: PxSequentialFile
-- Transformation: Count_Transactions | Type: PxAggregator
-- Target: Count_Customers_Transactions | Type: PxSequentialFile

-- Version 1: Initial conversion from DataStage DSX format
-- Job Flow: RETAIL_DATA_MART → Count_Transactions → Count_Customers_Transactions
-- Aggregation: GROUP BY CustomerID, Stockid with COUNT(*) as total_orders_num

WITH source_data AS (
    -- Source: RETAIL_DATA_MART | Field: All fields | Type: Various
    SELECT 
        productid,
        productname,
        category,
        subcategory,
        sales,
        quantity,
        customerid,  -- Source: RETAIL_DATA_MART | Field: CustomerID | Type: Integer
        customername,
        spendingscore,
        annualincomek,
        gender,
        age,
        customertype,
        stockid,     -- Source: RETAIL_DATA_MART | Field: Stockid | Type: Integer
        name,
        rating,
        location,
        noofemployees,
        invoiceid,
        description,
        invoicedate,
        price,
        country
    FROM {{ source('raw_data', 'retail_data_mart') }}
),

count_transactions AS (
    -- Transformation: Count_Transactions | Type: PxAggregator
    -- Method: sort | Keys: CustomerID, Stockid | Selection: count
    -- Source: Count_Transactions | Field: CustomerID | Type: Integer
    -- Source: Count_Transactions | Field: Stockid | Type: Integer  
    -- Source: Count_Transactions | Field: total_orders_num | Type: Integer
    SELECT 
        customerid,
        stockid,
        COUNT(*) AS total_orders_num  -- Derivation: RecCount() from DataStage
    FROM source_data
    GROUP BY 
        customerid,
        stockid
)

-- Final output matching DataStage target schema
-- Target: Count_Customers_Transactions | Output columns: CustomerID, Stockid, total_orders_num
SELECT 
    customerid,
    stockid, 
    total_orders_num
FROM count_transactions
ORDER BY customerid, stockid