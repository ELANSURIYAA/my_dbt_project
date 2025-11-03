-- Version 1: Initial DataStage to DBT Conversion
-- Source: RETAIL_DATA_MART_Job.dsx
-- Job: RETAIL_DATA_MART_Job
-- Description: Converts DataStage ETL job to DBT model for Snowflake
-- Date: 2023-05-25

{{ config(
    materialized='table',
    tags=['datastage_conversion', 'retail_data_mart'],
    pre_hook="
        -- Audit Framework: Insert job start record
        INSERT INTO {{ var('audit_schema', 'PUBLIC') }}.job_audit_log (
            batch_id,
            job_name,
            start_time,
            status
        )
        VALUES (
            '{{ var(\"batch_id\", \'BATCH_\' || TO_CHAR(CURRENT_TIMESTAMP(), \'YYYYMMDDHH24MISS\')) }}',
            'RETAIL_DATA_MART_Job',
            CURRENT_TIMESTAMP(),
            'RUNNING'
        );
    ",
    post_hook=[
        "
        -- Audit Framework: Update job completion record
        UPDATE {{ var('audit_schema', 'PUBLIC') }}.job_audit_log
        SET 
            end_time = CURRENT_TIMESTAMP(),
            status = 'SUCCESS',
            source_count = (SELECT COUNT(*) FROM {{ ref('stg_transactions_fact') }}),
            target_inserts = (SELECT COUNT(*) FROM {{ this }}),
            target_updates = 0
        WHERE batch_id = '{{ var(\"batch_id\", \'BATCH_\' || TO_CHAR(CURRENT_TIMESTAMP(), \'YYYYMMDDHH24MISS\')) }}'
        AND job_name = 'RETAIL_DATA_MART_Job';
        "
    ]
) }}

/*
=============================================================================
DATASTAGE TO DBT CONVERSION - RETAIL DATA MART
=============================================================================

DataStage Job Flow:
1. Customer_Dim (Source) → Customer_Transactions (Join)
2. Transactions_Fact (Source) → Customer_Transactions (Join)
3. Retail_Dim (Source) → Customer_Transactions_Retail (Join)
4. Customer_Transactions → Customer_Transactions_Retail (Join on Stockid)
5. Product_Dim (Source) → Customer_Transactions_Retail_Product (Join)
6. Customer_Transactions_Retail → Customer_Transactions_Retail_Product (Join on productid)
7. Joined_Data → Filtered_Data (Filter: customertype like 'citizen' or 'foriegn')
8. Filtered_Data → Transform_Data (Transform: UpCase(customername))
9. Transform_Data → ACTIVATIONSALES_DATA_MART (Target)

Key Transformations:
- InvoiceDate: Converted from string to int32 in Customer_Transactions join
- customername: Converted to uppercase in Transform_Data stage
- Filter: customertype IN ('citizen', 'foriegn')

Join Keys:
- Customer_Transactions: CustomerID
- Customer_Transactions_Retail: Stockid
- Customer_Transactions_Retail_Product: productid

=============================================================================
*/

WITH 

-- Source: Customer_Dim | Stage: PxSequentialFile | File: customer.txt
-- Columns: CustomerID, customername, SpendingScore, AnnualIncomek, gender, age, customertype
stg_customer_dim AS (
    SELECT
        customer_id,
        customername,
        spending_score,
        annual_incomek,
        gender,
        age,
        customertype
    FROM {{ source('PUBLIC', 'CUSTOMER_DIM') }}
    WHERE customer_id IS NOT NULL  -- Data quality check
),

-- Source: Retail_Dim | Stage: PxSequentialFile | File: Retail.txt
-- Columns: Stockid, name, rating, location, noofemployees
stg_retail_dim AS (
    SELECT
        stockid,
        name,
        rating,
        location,
        noofemployees
    FROM {{ source('PUBLIC', 'RETAIL_DIM') }}
    WHERE stockid IS NOT NULL  -- Data quality check
),

-- Source: Transactions_Fact | Stage: PxSequentialFile | File: transactiondata.txt
-- Columns: Stockid, Invoiceid, Description, Quantity, InvoiceDate, Price, CustomerID, Country, productid
stg_transactions_fact AS (
    SELECT
        stockid,
        invoiceid,
        description,
        quantity,
        -- DataStage Transformation: InvoiceDate converted from string to int32
        TRY_CAST(invoice_date AS INTEGER) AS invoice_date,
        price,
        customer_id,
        country,
        productid
    FROM {{ source('PUBLIC', 'TRANSACTIONS_FACT') }}
    WHERE customer_id IS NOT NULL  -- Data quality check
      AND stockid IS NOT NULL
      AND productid IS NOT NULL
),

-- Source: Product_Dim | Stage: PxSequentialFile | File: product.txt
-- Columns: productid, productname, Category, SubCategory, Sales, Quantity
stg_product_dim AS (
    SELECT
        productid,
        productname,
        category,
        subcategory,
        sales,
        quantity
    FROM {{ source('PUBLIC', 'PRODUCT_DIM') }}
    WHERE productid IS NOT NULL  -- Data quality check
),

-- Stage: Customer_Transactions | Operator: innerjoin | Key: CustomerID
-- Joins Transactions_Fact with Customer_Dim
customer_transactions AS (
    SELECT
        -- Customer dimension fields
        c.customer_id,
        c.customername,
        c.spending_score,
        c.annual_incomek,
        c.gender,
        c.age,
        c.customertype,
        -- Transaction fact fields
        t.stockid,
        t.invoiceid,
        t.description,
        t.quantity,
        t.invoice_date,
        t.price,
        t.country,
        t.productid
    FROM stg_transactions_fact t
    INNER JOIN stg_customer_dim c
        ON t.customer_id = c.customer_id
),

-- Stage: Customer_Transactions_Retail | Operator: innerjoin | Key: Stockid
-- Joins Customer_Transactions with Retail_Dim
customer_transactions_retail AS (
    SELECT
        -- Customer and transaction fields
        ct.customer_id,
        ct.customername,
        ct.spending_score,
        ct.annual_incomek,
        ct.gender,
        ct.age,
        ct.customertype,
        -- Retail dimension fields
        r.stockid,
        r.name,
        r.rating,
        r.location,
        r.noofemployees,
        -- Transaction fields
        ct.invoiceid,
        ct.description,
        ct.quantity,
        ct.invoice_date,
        ct.price,
        ct.country,
        ct.productid
    FROM customer_transactions ct
    INNER JOIN stg_retail_dim r
        ON ct.stockid = r.stockid
),

-- Stage: Customer_Transactions_Retail_Product | Operator: innerjoin | Key: productid
-- Joins Customer_Transactions_Retail with Product_Dim
customer_transactions_retail_product AS (
    SELECT
        -- Product dimension fields
        p.productid,
        p.productname,
        p.category,
        p.subcategory,
        p.sales,
        p.quantity,
        -- Customer fields
        ctr.customer_id,
        ctr.customername,
        ctr.spending_score,
        ctr.annual_incomek,
        ctr.gender,
        ctr.age,
        ctr.customertype,
        -- Retail fields
        ctr.stockid,
        ctr.name,
        ctr.rating,
        ctr.location,
        ctr.noofemployees,
        -- Transaction fields
        ctr.invoiceid,
        ctr.description,
        ctr.invoice_date,
        ctr.price,
        ctr.country
    FROM customer_transactions_retail ctr
    INNER JOIN stg_product_dim p
        ON ctr.productid = p.productid
),

-- Stage: Filtered_Data | Operator: filter
-- Filter: customertype like "citizen" or customertype like "foriegn"
filtered_data AS (
    SELECT *
    FROM customer_transactions_retail_product
    WHERE LOWER(customertype) LIKE '%citizen%' 
       OR LOWER(customertype) LIKE '%foriegn%'
),

-- Stage: Transform_Data | Operator: transform
-- Transformation: customername = UpCase(customername)
transformed_data AS (
    SELECT
        productid,
        productname,
        category,
        subcategory,
        sales,
        quantity,
        customer_id,
        -- DataStage Transformation: UpCase(customername)
        UPPER(customername) AS customername,
        spending_score,
        annual_incomek,
        gender,
        age,
        customertype,
        stockid,
        name,
        rating,
        location,
        noofemployees,
        invoiceid,
        description,
        invoice_date,
        price,
        country
    FROM filtered_data
)

-- Final output matching DataStage target: ACTIVATIONSALES_DATA_MART
SELECT
    productid,
    productname,
    category,
    subcategory,
    sales,
    quantity,
    customer_id,
    customername,
    spending_score,
    annual_incomek,
    gender,
    age,
    customertype,
    stockid,
    name,
    rating,
    location,
    noofemployees,
    invoiceid,
    description,
    invoice_date,
    price,
    country
FROM transformed_data

/*
=============================================================================
AUDIT & ERROR HANDLING NOTES
=============================================================================

Audit Framework:
- Pre-hook: Inserts job start record into job_audit_log table
- Post-hook: Updates job completion with row counts and status
- Batch ID: Generated using timestamp or passed as variable

Error Handling:
- NULL checks on join keys (customer_id, stockid, productid)
- TRY_CAST for invoice_date conversion to handle invalid data
- Filter validation for customertype values

Reject Handling:
- Rejected rows from data quality checks can be captured in separate reject tables
- Use dbt tests in model.yml for additional validation

Job Parameters (DBT Variables):
- batch_id: Unique identifier for job run
- audit_schema: Schema for audit tables (default: PUBLIC)
- run_date: Execution date for incremental loads

Partitioning Strategy:
- Recommended: CLUSTER BY (invoice_date, customer_id) for query performance
- Hash partitioning on natural keys as per DataStage design

=============================================================================
*/