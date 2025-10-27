-- Version 1: Initial conversion from DataStage RETAIL_DATA_MART_Job
-- Source: DataStage Job RETAIL_DATA_MART_Job | Converted to DBT
-- Job Flow: Customer_Dim + Transactions_Fact + Retail_Dim + Product_Dim → Joins → Filter → Transform → retail_data_mart

{{ config(
    materialized='table',
    tags=['datastage_conversion', 'retail_data_mart'],
    database='AVA_DB',
    schema='PUBLIC'
) }}

-- Stage 1: Customer_Transactions (Join Transactions_Fact with Customer_Dim on CustomerID)
-- Source: Transactions_Fact | Field: CustomerID | Type: Integer
WITH customer_transactions AS (
    SELECT
        -- From Customer_Dim
        c.customer_id AS CustomerID,
        c.customername,
        c.spending_score AS SpendingScore,
        c.annual_incomek AS AnnualIncomek,
        c.gender,
        c.age,
        c.customertype,
        -- From Transactions_Fact
        t.stockid AS Stockid,
        t.invoiceid AS Invoiceid,
        t.description AS Description,
        t.quantity AS Quantity,
        CAST(t.invoice_date AS INTEGER) AS InvoiceDate,  -- DataStage converts string to int32
        t.price AS Price,
        t.country AS Country,
        t.productid AS productid
    FROM {{ source('PUBLIC', 'transactions_fact') }} t
    INNER JOIN {{ source('PUBLIC', 'customer_dim') }} c
        ON t.customer_id = c.customer_id
),

-- Stage 2: Customer_Transactions_Retail (Join with Retail_Dim on Stockid)
-- Source: Retail_Dim | Field: Stockid | Type: Integer
customer_transactions_retail AS (
    SELECT
        ct.CustomerID,
        ct.customername,
        ct.SpendingScore,
        ct.AnnualIncomek,
        ct.gender,
        ct.age,
        ct.customertype,
        -- From Retail_Dim
        r.stockid AS Stockid,
        r.name,
        r.rating,
        r.location,
        r.noofemployees,
        -- From Customer_Transactions
        ct.Invoiceid,
        ct.Description,
        ct.Quantity,
        ct.InvoiceDate,
        ct.Price,
        ct.Country,
        ct.productid
    FROM customer_transactions ct
    INNER JOIN {{ source('PUBLIC', 'retail_dim') }} r
        ON ct.Stockid = r.stockid
),

-- Stage 3: Customer_Transactions_Retail_Product (Join with Product_Dim on productid)
-- Source: Product_Dim | Field: productid | Type: Integer
customer_transactions_retail_product AS (
    SELECT
        -- From Product_Dim
        p.productid,
        p.productname,
        p.category AS Category,
        p.subcategory AS SubCategory,
        p.sales AS Sales,
        p.quantity AS Quantity,
        -- From Customer_Transactions_Retail
        ctr.CustomerID,
        ctr.customername,
        ctr.SpendingScore,
        ctr.AnnualIncomek,
        ctr.gender,
        ctr.age,
        ctr.customertype,
        ctr.Stockid,
        ctr.name,
        ctr.rating,
        ctr.location,
        ctr.noofemployees,
        ctr.Invoiceid,
        ctr.Description,
        ctr.InvoiceDate,
        ctr.Price,
        ctr.Country
    FROM customer_transactions_retail ctr
    INNER JOIN {{ source('PUBLIC', 'product_dim') }} p
        ON ctr.productid = p.productid
),

-- Stage 4: Filtered_Data (Filter stage)
-- Filter: customertype like "citizen" or customertype like "foriegn"
filtered_data AS (
    SELECT
        productid,
        productname,
        Category,
        SubCategory,
        Sales,
        Quantity,
        CustomerID,
        customername,
        SpendingScore,
        AnnualIncomek,
        gender,
        age,
        customertype,
        Stockid,
        name,
        rating,
        location,
        noofemployees,
        Invoiceid,
        Description,
        InvoiceDate,
        Price,
        Country
    FROM customer_transactions_retail_product
    WHERE LOWER(customertype) LIKE '%citizen%' 
       OR LOWER(customertype) LIKE '%foriegn%'
)

-- Stage 5: Transform_Data (Transformer stage)
-- Transformation: customername = UpCase(customername)
-- Final output to ACTIVATIONSALES_DATA_MART
SELECT
    productid,
    productname,
    Category,
    SubCategory,
    Sales,
    Quantity,
    CustomerID,
    UPPER(customername) AS customername,  -- DataStage UpCase() transformation
    SpendingScore,
    AnnualIncomek,
    gender,
    age,
    customertype,
    Stockid,
    name,
    rating,
    location,
    noofemployees,
    Invoiceid,
    Description,
    InvoiceDate,
    Price,
    Country
FROM filtered_data