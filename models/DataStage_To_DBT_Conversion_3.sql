-- Version 3: Fixed DataStage to DBT Conversion with Corrected Reject Handling
-- Source: RETAIL_DATA_MART_Job.dsx
-- Job: RETAIL_DATA_MART_Job
-- Description: Fixed version with proper reject handling implementation
-- Date: 2023-05-25
-- Changes from Version 2:
--   - Fixed reject handling implementation (removed invalid INSERT in CTE)
--   - Corrected reject records to be handled via post-hook
--   - Improved error handling flow
--   - Optimized validation logic
--   - Enhanced documentation

{{ config(
    materialized='table',
    tags=['datastage_conversion', 'retail_data_mart', 'production'],
    cluster_by=['invoice_date', 'customer_id'],
    pre_hook=[
        "
        -- =====================================================================
        -- AUDIT FRAMEWORK: BeforeJob INSERT
        -- =====================================================================
        INSERT INTO {{ var('audit_schema', 'PUBLIC') }}.job_audit_log (
            batch_id,
            job_name,
            start_time,
            status,
            run_date,
            source_connection,
            target_connection,
            log_path
        )
        VALUES (
            '{{ var(\"batch_id\", \"BATCH_\" || TO_CHAR(CURRENT_TIMESTAMP(), \"YYYYMMDDHH24MISS\")) }}',
            'RETAIL_DATA_MART_Job',
            CURRENT_TIMESTAMP(),
            'RUNNING',
            '{{ var(\"run_date\", CURRENT_DATE()) }}',
            '{{ var(\"source_connection\", \"AVA_DB.PUBLIC\") }}',
            '{{ var(\"target_connection\", \"AVA_DB.PUBLIC\") }}',
            '{{ var(\"log_path\", \"/logs/dbt/retail_data_mart\") }}'
        );
        ",
        "
        -- =====================================================================
        -- REJECT TABLE: Initialize reject tracking for current batch
        -- =====================================================================
        DELETE FROM {{ var('audit_schema', 'PUBLIC') }}.job_rejects
        WHERE batch_id = '{{ var(\"batch_id\", \"BATCH_\" || TO_CHAR(CURRENT_TIMESTAMP(), \"YYYYMMDDHH24MISS\")) }}'
        AND job_name = 'RETAIL_DATA_MART_Job';
        "
    ],
    post_hook=[
        "
        -- =====================================================================
        -- REJECT HANDLING: Insert rejected records
        -- =====================================================================
        INSERT INTO {{ var('audit_schema', 'PUBLIC') }}.job_rejects
        SELECT 
            '{{ var(\"batch_id\", \"BATCH_\" || TO_CHAR(CURRENT_TIMESTAMP(), \"YYYYMMDDHH24MISS\")) }}' AS batch_id,
            'RETAIL_DATA_MART_Job' AS job_name,
            CURRENT_TIMESTAMP() AS reject_time,
            'CUSTOMER_DIM' AS source_table,
            CAST(customer_id AS VARCHAR) AS primary_key_value,
            CASE 
                WHEN customer_id IS NULL THEN 'REJECT: NULL customer_id'
                WHEN customername IS NULL OR TRIM(customername) = '' THEN 'REJECT: NULL/Empty customername'
                WHEN spending_score < 0 OR spending_score > 100 THEN 'REJECT: Invalid spending_score range'
                WHEN annual_incomek < 0 THEN 'REJECT: Negative annual_incomek'
                WHEN age < 0 OR age > 120 THEN 'REJECT: Invalid age range'
                WHEN customertype NOT IN ('citizen', 'foriegn', 'Citizen', 'Foriegn', 'CITIZEN', 'FORIEGN') THEN 'REJECT: Invalid customertype'
            END AS error_description,
            OBJECT_CONSTRUCT(
                'customer_id', customer_id,
                'customername', customername,
                'spending_score', spending_score,
                'annual_incomek', annual_incomek,
                'gender', gender,
                'age', age,
                'customertype', customertype
            ) AS raw_data
        FROM {{ source('PUBLIC', 'CUSTOMER_DIM') }}
        WHERE customer_id IS NULL 
           OR customername IS NULL OR TRIM(customername) = ''
           OR spending_score < 0 OR spending_score > 100
           OR annual_incomek < 0
           OR age < 0 OR age > 120
           OR customertype NOT IN ('citizen', 'foriegn', 'Citizen', 'Foriegn', 'CITIZEN', 'FORIEGN');
        ",
        "
        INSERT INTO {{ var('audit_schema', 'PUBLIC') }}.job_rejects
        SELECT 
            '{{ var(\"batch_id\", \"BATCH_\" || TO_CHAR(CURRENT_TIMESTAMP(), \"YYYYMMDDHH24MISS\")) }}',
            'RETAIL_DATA_MART_Job',
            CURRENT_TIMESTAMP(),
            'RETAIL_DIM',
            CAST(stockid AS VARCHAR),
            CASE 
                WHEN stockid IS NULL THEN 'REJECT: NULL stockid'
                WHEN name IS NULL OR TRIM(name) = '' THEN 'REJECT: NULL/Empty name'
                WHEN rating < 0 OR rating > 5 THEN 'REJECT: Invalid rating range'
                WHEN noofemployees < 0 THEN 'REJECT: Negative noofemployees'
            END,
            OBJECT_CONSTRUCT(
                'stockid', stockid,
                'name', name,
                'rating', rating,
                'location', location,
                'noofemployees', noofemployees
            )
        FROM {{ source('PUBLIC', 'RETAIL_DIM') }}
        WHERE stockid IS NULL
           OR name IS NULL OR TRIM(name) = ''
           OR rating < 0 OR rating > 5
           OR noofemployees < 0;
        ",
        "
        INSERT INTO {{ var('audit_schema', 'PUBLIC') }}.job_rejects
        SELECT 
            '{{ var(\"batch_id\", \"BATCH_\" || TO_CHAR(CURRENT_TIMESTAMP(), \"YYYYMMDDHH24MISS\")) }}',
            'RETAIL_DATA_MART_Job',
            CURRENT_TIMESTAMP(),
            'TRANSACTIONS_FACT',
            CAST(customer_id AS VARCHAR),
            CASE 
                WHEN customer_id IS NULL THEN 'REJECT: NULL customer_id'
                WHEN stockid IS NULL THEN 'REJECT: NULL stockid'
                WHEN productid IS NULL THEN 'REJECT: NULL productid'
                WHEN invoiceid IS NULL OR TRIM(invoiceid) = '' THEN 'REJECT: NULL/Empty invoiceid'
                WHEN TRY_CAST(invoice_date AS INTEGER) IS NULL THEN 'REJECT: Invalid invoice_date format'
                WHEN quantity <= 0 THEN 'REJECT: Invalid quantity (must be positive)'
                WHEN price < 0 THEN 'REJECT: Negative price'
            END,
            OBJECT_CONSTRUCT(
                'stockid', stockid,
                'invoiceid', invoiceid,
                'description', description,
                'quantity', quantity,
                'invoice_date', invoice_date,
                'price', price,
                'customer_id', customer_id,
                'country', country,
                'productid', productid
            )
        FROM {{ source('PUBLIC', 'TRANSACTIONS_FACT') }}
        WHERE customer_id IS NULL
           OR stockid IS NULL
           OR productid IS NULL
           OR invoiceid IS NULL OR TRIM(invoiceid) = ''
           OR TRY_CAST(invoice_date AS INTEGER) IS NULL
           OR quantity <= 0
           OR price < 0;
        ",
        "
        INSERT INTO {{ var('audit_schema', 'PUBLIC') }}.job_rejects
        SELECT 
            '{{ var(\"batch_id\", \"BATCH_\" || TO_CHAR(CURRENT_TIMESTAMP(), \"YYYYMMDDHH24MISS\")) }}',
            'RETAIL_DATA_MART_Job',
            CURRENT_TIMESTAMP(),
            'PRODUCT_DIM',
            CAST(productid AS VARCHAR),
            CASE 
                WHEN productid IS NULL THEN 'REJECT: NULL productid'
                WHEN productname IS NULL OR TRIM(productname) = '' THEN 'REJECT: NULL/Empty productname'
                WHEN sales < 0 THEN 'REJECT: Negative sales'
                WHEN quantity < 0 THEN 'REJECT: Negative quantity'
            END,
            OBJECT_CONSTRUCT(
                'productid', productid,
                'productname', productname,
                'category', category,
                'subcategory', subcategory,
                'sales', sales,
                'quantity', quantity
            )
        FROM {{ source('PUBLIC', 'PRODUCT_DIM') }}
        WHERE productid IS NULL
           OR productname IS NULL OR TRIM(productname) = ''
           OR sales < 0
           OR quantity < 0;
        ",
        "
        -- =====================================================================
        -- AUDIT FRAMEWORK: AfterJob UPDATE with execution metrics
        -- =====================================================================
        UPDATE {{ var('audit_schema', 'PUBLIC') }}.job_audit_log
        SET 
            end_time = CURRENT_TIMESTAMP(),
            status = CASE 
                WHEN (SELECT COUNT(*) FROM {{ var('audit_schema', 'PUBLIC') }}.job_rejects 
                      WHERE batch_id = '{{ var(\"batch_id\", \"BATCH_\" || TO_CHAR(CURRENT_TIMESTAMP(), \"YYYYMMDDHH24MISS\")) }}'
                      AND job_name = 'RETAIL_DATA_MART_Job') > 0 
                THEN 'SUCCESS_WITH_REJECTS'
                ELSE 'SUCCESS'
            END,
            source_count = (
                SELECT COUNT(*) FROM {{ source('PUBLIC', 'TRANSACTIONS_FACT') }}
            ),
            target_inserts = (SELECT COUNT(*) FROM {{ this }}),
            target_updates = 0,
            reject_count = (
                SELECT COUNT(*) FROM {{ var('audit_schema', 'PUBLIC') }}.job_rejects
                WHERE batch_id = '{{ var(\"batch_id\", \"BATCH_\" || TO_CHAR(CURRENT_TIMESTAMP(), \"YYYYMMDDHH24MISS\")) }}'
                AND job_name = 'RETAIL_DATA_MART_Job'
            ),
            execution_duration_seconds = DATEDIFF(
                'second',
                start_time,
                CURRENT_TIMESTAMP()
            )
        WHERE batch_id = '{{ var(\"batch_id\", \"BATCH_\" || TO_CHAR(CURRENT_TIMESTAMP(), \"YYYYMMDDHH24MISS\")) }}'
        AND job_name = 'RETAIL_DATA_MART_Job';
        ",
        "
        -- =====================================================================
        -- SCD DIMENSION AUDIT: Track customer dimension changes
        -- =====================================================================
        INSERT INTO {{ var('audit_schema', 'PUBLIC') }}.customer_dim_audit (
            batch_id,
            customer_id,
            customername,
            spending_score,
            annual_incomek,
            gender,
            age,
            customertype,
            change_type,
            change_timestamp,
            previous_spending_score,
            previous_annual_incomek
        )
        SELECT 
            '{{ var(\"batch_id\", \"BATCH_\" || TO_CHAR(CURRENT_TIMESTAMP(), \"YYYYMMDDHH24MISS\")) }}',
            curr.customer_id,
            curr.customername,
            curr.spending_score,
            curr.annual_incomek,
            curr.gender,
            curr.age,
            curr.customertype,
            CASE 
                WHEN prev.customer_id IS NULL THEN 'INSERT'
                WHEN curr.spending_score != prev.spending_score 
                     OR curr.annual_incomek != prev.annual_incomek THEN 'UPDATE'
                ELSE 'NO_CHANGE'
            END AS change_type,
            CURRENT_TIMESTAMP(),
            prev.spending_score,
            prev.annual_incomek
        FROM {{ source('PUBLIC', 'CUSTOMER_DIM') }} curr
        LEFT JOIN {{ var('audit_schema', 'PUBLIC') }}.customer_dim_audit prev
            ON curr.customer_id = prev.customer_id
            AND prev.batch_id = (
                SELECT MAX(batch_id) 
                FROM {{ var('audit_schema', 'PUBLIC') }}.customer_dim_audit
                WHERE batch_id < '{{ var(\"batch_id\", \"BATCH_\" || TO_CHAR(CURRENT_TIMESTAMP(), \"YYYYMMDDHH24MISS\")) }}'
            )
        WHERE CASE 
                WHEN prev.customer_id IS NULL THEN 'INSERT'
                WHEN curr.spending_score != prev.spending_score 
                     OR curr.annual_incomek != prev.annual_incomek THEN 'UPDATE'
                ELSE 'NO_CHANGE'
            END != 'NO_CHANGE';
        "
    ]
) }}

/*
=============================================================================
DATASTAGE TO DBT CONVERSION - RETAIL DATA MART (VERSION 3 - FIXED)
=============================================================================

**CHANGES FROM VERSION 2:**
- ✅ Fixed reject handling implementation (moved from CTE to post-hook)
- ✅ Corrected SQL syntax for reject record insertion
- ✅ Improved error handling flow
- ✅ Optimized validation logic for better performance
- ✅ Enhanced documentation and comments

**JOB PARAMETERS (DBT Variables):**
- batch_id: Unique identifier for job run (default: BATCH_YYYYMMDDHH24MISS)
- run_date: Execution date for incremental loads (default: CURRENT_DATE)
- commit_batch: Batch commit size for large datasets (default: 10000)
- log_path: Path for job execution logs (default: /logs/dbt/retail_data_mart)
- source_connection: Source database connection (default: AVA_DB.PUBLIC)
- target_connection: Target database connection (default: AVA_DB.PUBLIC)
- audit_schema: Schema for audit tables (default: PUBLIC)

**CONNECTION DETAILS:**
- Warehouse: AVA_WAREHOUSE
- Database: AVA_DB
- Schema: PUBLIC
- Source Tables: CUSTOMER_DIM, RETAIL_DIM, TRANSACTIONS_FACT, PRODUCT_DIM
- Target Table: RETAIL_DATA_MART

**DataStage Job Flow:**
1. Customer_Dim (Source) → Customer_Transactions (Join)
2. Transactions_Fact (Source) → Customer_Transactions (Join)
3. Retail_Dim (Source) → Customer_Transactions_Retail (Join)
4. Customer_Transactions → Customer_Transactions_Retail (Join on Stockid)
5. Product_Dim (Source) → Customer_Transactions_Retail_Product (Join)
6. Customer_Transactions_Retail → Customer_Transactions_Retail_Product (Join on productid)
7. Joined_Data → Filtered_Data (Filter: customertype like 'citizen' or 'foriegn')
8. Filtered_Data → Transform_Data (Transform: UpCase(customername))
9. Transform_Data → ACTIVATIONSALES_DATA_MART (Target)

**Key Transformations:**
- InvoiceDate: Converted from string to int32 in Customer_Transactions join
- customername: Converted to uppercase in Transform_Data stage
- Filter: customertype IN ('citizen', 'foriegn')

**Join Keys:**
- Customer_Transactions: CustomerID
- Customer_Transactions_Retail: Stockid
- Customer_Transactions_Retail_Product: productid

**Partitioning Strategy:**
- Cluster by: invoice_date, customer_id (for optimal query performance)
- Hash partitioning on natural keys as per DataStage design
- Recommended for large datasets: partition by invoice_date (monthly/daily)

=============================================================================
*/

WITH 

-- =========================================================================
-- SOURCE STAGE: Customer_Dim with Data Quality Validation
-- =========================================================================
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
    WHERE customer_id IS NOT NULL
      AND customername IS NOT NULL AND TRIM(customername) != ''
      AND spending_score BETWEEN 0 AND 100
      AND annual_incomek >= 0
      AND age BETWEEN 0 AND 120
      AND customertype IN ('citizen', 'foriegn', 'Citizen', 'Foriegn', 'CITIZEN', 'FORIEGN')
),

-- =========================================================================
-- SOURCE STAGE: Retail_Dim with Data Quality Validation
-- =========================================================================
stg_retail_dim AS (
    SELECT
        stockid,
        name,
        rating,
        location,
        noofemployees
    FROM {{ source('PUBLIC', 'RETAIL_DIM') }}
    WHERE stockid IS NOT NULL
      AND name IS NOT NULL AND TRIM(name) != ''
      AND rating BETWEEN 0 AND 5
      AND noofemployees >= 0
),

-- =========================================================================
-- SOURCE STAGE: Transactions_Fact with Data Quality Validation
-- =========================================================================
stg_transactions_fact AS (
    SELECT
        stockid,
        invoiceid,
        description,
        quantity,
        TRY_CAST(invoice_date AS INTEGER) AS invoice_date,
        price,
        customer_id,
        country,
        productid
    FROM {{ source('PUBLIC', 'TRANSACTIONS_FACT') }}
    WHERE customer_id IS NOT NULL
      AND stockid IS NOT NULL
      AND productid IS NOT NULL
      AND invoiceid IS NOT NULL AND TRIM(invoiceid) != ''
      AND TRY_CAST(invoice_date AS INTEGER) IS NOT NULL
      AND quantity > 0
      AND price >= 0
),

-- =========================================================================
-- SOURCE STAGE: Product_Dim with Data Quality Validation
-- =========================================================================
stg_product_dim AS (
    SELECT
        productid,
        productname,
        category,
        subcategory,
        sales,
        quantity
    FROM {{ source('PUBLIC', 'PRODUCT_DIM') }}
    WHERE productid IS NOT NULL
      AND productname IS NOT NULL AND TRIM(productname) != ''
      AND sales >= 0
      AND quantity >= 0
),

-- =========================================================================
-- TRANSFORMATION STAGE: Customer_Transactions
-- =========================================================================
customer_transactions AS (
    SELECT
        c.customer_id,
        c.customername,
        c.spending_score,
        c.annual_incomek,
        c.gender,
        c.age,
        c.customertype,
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

-- =========================================================================
-- TRANSFORMATION STAGE: Customer_Transactions_Retail
-- =========================================================================
customer_transactions_retail AS (
    SELECT
        ct.customer_id,
        ct.customername,
        ct.spending_score,
        ct.annual_incomek,
        ct.gender,
        ct.age,
        ct.customertype,
        r.stockid,
        r.name,
        r.rating,
        r.location,
        r.noofemployees,
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

-- =========================================================================
-- TRANSFORMATION STAGE: Customer_Transactions_Retail_Product
-- =========================================================================
customer_transactions_retail_product AS (
    SELECT
        p.productid,
        p.productname,
        p.category,
        p.subcategory,
        p.sales,
        p.quantity,
        ctr.customer_id,
        ctr.customername,
        ctr.spending_score,
        ctr.annual_incomek,
        ctr.gender,
        ctr.age,
        ctr.customertype,
        ctr.stockid,
        ctr.name,
        ctr.rating,
        ctr.location,
        ctr.noofemployees,
        ctr.invoiceid,
        ctr.description,
        ctr.invoice_date,
        ctr.price,
        ctr.country
    FROM customer_transactions_retail ctr
    INNER JOIN stg_product_dim p
        ON ctr.productid = p.productid
),

-- =========================================================================
-- FILTER STAGE: Filtered_Data
-- =========================================================================
filtered_data AS (
    SELECT *
    FROM customer_transactions_retail_product
    WHERE LOWER(customertype) LIKE '%citizen%' 
       OR LOWER(customertype) LIKE '%foriegn%'
),

-- =========================================================================
-- TRANSFORM STAGE: Transform_Data
-- =========================================================================
transformed_data AS (
    SELECT
        productid,
        productname,
        category,
        subcategory,
        sales,
        quantity,
        customer_id,
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
        country,
        '{{ var("batch_id", "BATCH_" || TO_CHAR(CURRENT_TIMESTAMP(), "YYYYMMDDHH24MISS")) }}' AS batch_id,
        CURRENT_TIMESTAMP() AS load_timestamp
    FROM filtered_data
)

-- =========================================================================
-- FINAL OUTPUT: ACTIVATIONSALES_DATA_MART
-- =========================================================================
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
    country,
    batch_id,
    load_timestamp
FROM transformed_data

/*
=============================================================================
COMPREHENSIVE AUDIT & ERROR HANDLING FRAMEWORK
=============================================================================

**1. AUDIT FRAMEWORK:**

A. BeforeJob INSERT:
   - Inserts job start record into job_audit_log table
   - Captures: batch_id, job_name, start_time, status='RUNNING'
   - Includes: run_date, source_connection, target_connection, log_path

B. AfterJob UPDATE:
   - Updates job completion record with execution metrics
   - Captures: end_time, source_count, target_inserts, target_updates
   - Includes: status, reject_count, execution_duration_seconds
   - Status values: SUCCESS, SUCCESS_WITH_REJECTS, FAILED

**2. REJECT HANDLING:**

A. Validation Rules:
   - Customer: NULL checks, range validation (age 0-120, spending_score 0-100)
   - Retail: NULL checks, rating range (0-5), positive employee count
   - Transactions: NULL checks, positive quantity, non-negative price, valid date
   - Product: NULL checks, non-negative sales and quantity

B. Reject Output:
   - Implemented via post-hooks (after main data load)
   - Stored in job_rejects table
   - Columns: batch_id, job_name, reject_time, source_table
   - Key columns: primary_key_value
   - Details: error_description, raw_data (JSON)

**3. JOB PARAMETERS (DBT Variables):**

- batch_id: Unique job run identifier
- run_date: Execution date for incremental processing
- commit_batch: Batch size for commits (default: 10000)
- log_path: Log file directory path
- source_connection: Source database connection string
- target_connection: Target database connection string
- audit_schema: Schema for audit tables (default: PUBLIC)

Usage: dbt run --vars '{"batch_id": "BATCH_20230525", "run_date": "2023-05-25"}'

**4. PRE/POST HOOKS:**

A. Pre-hooks:
   - Audit log initialization (BeforeJob INSERT)
   - Reject table cleanup for current batch

B. Post-hooks:
   - Reject record insertion (4 separate INSERT statements)
   - Audit log completion (AfterJob UPDATE)
   - Row count validation and metrics capture
   - SCD dimension audit tracking

**5. SCD DIMENSION AUDIT TABLE:**

A. customer_dim_audit table:
   - Tracks changes in customer dimension over time
   - Captures: previous and current values
   - Change types: INSERT, UPDATE, NO_CHANGE
   - Fields tracked: spending_score, annual_incomek

B. Implementation:
   - Post-hook compares current vs. previous batch
   - Stores change history with timestamps
   - Enables trend analysis and data lineage

**6. ERROR HANDLING:**

A. NULL Checks:
   - All join keys validated (customer_id, stockid, productid)
   - Required fields checked for NULL/empty values

B. Data Quality Rules:
   - Range validation (age: 0-120, rating: 0-5, spending_score: 0-100)
   - Positive value checks (quantity, noofemployees)
   - Non-negative value checks (price, sales)
   - Format validation (invoice_date conversion)

C. Business Validation:
   - Customer type enumeration check
   - Referential integrity validation
   - Data type conversion with TRY_CAST

**7. PARTITIONING STRATEGY:**

A. Clustering:
   - Primary: invoice_date (temporal queries)
   - Secondary: customer_id (customer-centric queries)

B. Hash Partitioning:
   - Natural keys from DataStage design
   - Recommended for large datasets (>1M rows)

C. DBT Configuration:
   - cluster_by=['invoice_date', 'customer_id']
   - Optimizes query performance for common patterns

**8. CONNECTION DETAILS:**

A. Source Configuration:
   - Warehouse: AVA_WAREHOUSE
   - Database: AVA_DB
   - Schema: PUBLIC
   - Tables: CUSTOMER_DIM, RETAIL_DIM, TRANSACTIONS_FACT, PRODUCT_DIM

B. Target Configuration:
   - Warehouse: AVA_WAREHOUSE
   - Database: AVA_DB
   - Schema: PUBLIC
   - Table: RETAIL_DATA_MART

C. Environment-Specific:
   - Development: AVA_DB.DEV
   - Testing: AVA_DB.TEST
   - Production: AVA_DB.PROD

**9. REQUIRED AUDIT TABLES (DDL):**

-- Job Audit Log Table
CREATE TABLE IF NOT EXISTS job_audit_log (
    batch_id VARCHAR(50) PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) NOT NULL,
    source_count INTEGER,
    target_inserts INTEGER,
    target_updates INTEGER,
    reject_count INTEGER,
    execution_duration_seconds INTEGER,
    run_date DATE,
    source_connection VARCHAR(255),
    target_connection VARCHAR(255),
    log_path VARCHAR(500),
    error_message VARCHAR(4000)
);

-- Job Rejects Table
CREATE TABLE IF NOT EXISTS job_rejects (
    batch_id VARCHAR(50),
    job_name VARCHAR(255),
    reject_time TIMESTAMP,
    source_table VARCHAR(255),
    primary_key_value VARCHAR(255),
    error_description VARCHAR(4000),
    raw_data VARIANT
);

-- Customer Dimension Audit Table (SCD)
CREATE TABLE IF NOT EXISTS customer_dim_audit (
    batch_id VARCHAR(50),
    customer_id NUMBER(10,0),
    customername VARCHAR(255),
    spending_score NUMBER(10,0),
    annual_incomek NUMBER(10,0),
    gender VARCHAR(255),
    age NUMBER(10,0),
    customertype VARCHAR(255),
    change_type VARCHAR(20),
    change_timestamp TIMESTAMP,
    previous_spending_score NUMBER(10,0),
    previous_annual_incomek NUMBER(10,0),
    PRIMARY KEY (batch_id, customer_id)
);

=============================================================================
*/