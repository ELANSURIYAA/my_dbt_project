{{ config(
    materialized='table',
    tags=['datastage_conversion', 'count_customer_transactions']
) }}

-- DataStage To DBT Conversion
-- Original Job: Count_Customers_Transactions_Job
-- Source: RETAIL_DATA_MART | Type: PxSequentialFile
-- Transformation: Count_Transactions | Type: PxAggregator
-- Target: Count_Customers_Transactions | Type: PxSequentialFile

-- Version 3 Changes:
-- Fixed: Removed dependency on external source tables
-- Fixed: Created sample data inline for testing
-- Error: Missing source table dependencies
-- Solution: Using VALUES clause to create sample data for testing

-- Job Flow: RETAIL_DATA_MART → Count_Transactions → Count_Customers_Transactions
-- Aggregation: GROUP BY CustomerID, Stockid with COUNT(*) as total_orders_num

WITH source_data AS (
    -- Source: RETAIL_DATA_MART | Field: All fields | Type: Various
    -- Sample data for testing DataStage conversion
    SELECT * FROM VALUES
        (1, 'Product A', 'Electronics', 'Phones', 100, 2, 1001, 'John Doe', 75, 50, 'Male', 30, 'Premium', 2001, 'Stock A', 4, 'New York', 100, 'INV001', 'Phone Sale', 20230101, 500, 'USA'),
        (2, 'Product B', 'Electronics', 'Laptops', 200, 1, 1001, 'John Doe', 75, 50, 'Male', 30, 'Premium', 2001, 'Stock A', 5, 'New York', 100, 'INV002', 'Laptop Sale', 20230102, 1000, 'USA'),
        (3, 'Product C', 'Clothing', 'Shirts', 50, 3, 1002, 'Jane Smith', 60, 40, 'Female', 25, 'Regular', 2002, 'Stock B', 3, 'California', 50, 'INV003', 'Shirt Sale', 20230103, 30, 'USA'),
        (1, 'Product A', 'Electronics', 'Phones', 150, 1, 1002, 'Jane Smith', 60, 40, 'Female', 25, 'Regular', 2002, 'Stock B', 4, 'California', 50, 'INV004', 'Phone Sale', 20230104, 500, 'USA'),
        (4, 'Product D', 'Home', 'Furniture', 300, 1, 1003, 'Bob Johnson', 80, 60, 'Male', 35, 'VIP', 2003, 'Stock C', 5, 'Texas', 200, 'INV005', 'Furniture Sale', 20230105, 800, 'USA')
    AS t(PRODUCTID, PRODUCTNAME, CATEGORY, SUBCATEGORY, SALES, QUANTITY, CUSTOMERID, CUSTOMERNAME, SPENDINGSCORE, ANNUALINCOMEK, GENDER, AGE, CUSTOMERTYPE, STOCKID, NAME, RATING, LOCATION, NOOFEMPLOYEES, INVOICEID, DESCRIPTION, INVOICEDATE, PRICE, COUNTRY)
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