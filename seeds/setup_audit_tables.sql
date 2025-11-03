-- =====================================================
-- AUDIT FRAMEWORK SETUP FOR RETAIL DATA MART
-- =====================================================
-- This script creates the necessary audit and control tables
-- for the DataStage to DBT conversion project
-- =====================================================

-- Use the correct database and schema
USE DATABASE AVA_DB;
USE SCHEMA PUBLIC;

-- =====================================================
-- 1. JOB AUDIT LOG TABLE
-- =====================================================
-- Tracks job execution metrics and status
-- Used by pre_hook and post_hook in DBT models

CREATE TABLE IF NOT EXISTS job_audit_log (
    batch_id VARCHAR(50) PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL,
    start_time TIMESTAMP_NTZ NOT NULL,
    end_time TIMESTAMP_NTZ,
    status VARCHAR(20) NOT NULL,
    source_count INTEGER,
    target_inserts INTEGER,
    target_updates INTEGER,
    error_message VARCHAR(4000),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create index for performance
CREATE INDEX IF NOT EXISTS idx_job_audit_log_job_name 
    ON job_audit_log(job_name);

CREATE INDEX IF NOT EXISTS idx_job_audit_log_start_time 
    ON job_audit_log(start_time);

-- =====================================================
-- 2. JOB REJECTS TABLE
-- =====================================================
-- Stores rejected rows from data quality validations
-- Used for error handling and data quality monitoring

CREATE TABLE IF NOT EXISTS job_rejects (
    reject_id INTEGER AUTOINCREMENT PRIMARY KEY,
    batch_id VARCHAR(50) NOT NULL,
    job_name VARCHAR(255) NOT NULL,
    reject_time TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    source_table VARCHAR(255),
    primary_key_value VARCHAR(255),
    error_description VARCHAR(4000),
    raw_data VARIANT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_job_rejects_batch_id 
    ON job_rejects(batch_id);

CREATE INDEX IF NOT EXISTS idx_job_rejects_job_name 
    ON job_rejects(job_name);

CREATE INDEX IF NOT EXISTS idx_job_rejects_source_table 
    ON job_rejects(source_table);

-- =====================================================
-- 3. DIMENSION AUDIT TABLE (for SCD tracking)
-- =====================================================
-- Tracks changes to dimension tables over time
-- Used for Slowly Changing Dimension (SCD) management

CREATE TABLE IF NOT EXISTS dimension_audit (
    audit_id INTEGER AUTOINCREMENT PRIMARY KEY,
    dimension_name VARCHAR(255) NOT NULL,
    record_key VARCHAR(255) NOT NULL,
    change_type VARCHAR(20) NOT NULL, -- INSERT, UPDATE, DELETE
    change_timestamp TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    batch_id VARCHAR(50),
    old_values VARIANT,
    new_values VARIANT,
    changed_by VARCHAR(255),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_dimension_audit_dimension_name 
    ON dimension_audit(dimension_name);

CREATE INDEX IF NOT EXISTS idx_dimension_audit_record_key 
    ON dimension_audit(record_key);

CREATE INDEX IF NOT EXISTS idx_dimension_audit_change_timestamp 
    ON dimension_audit(change_timestamp);

-- =====================================================
-- 4. DATA QUALITY METRICS TABLE
-- =====================================================
-- Stores data quality check results
-- Used for monitoring and reporting

CREATE TABLE IF NOT EXISTS data_quality_metrics (
    metric_id INTEGER AUTOINCREMENT PRIMARY KEY,
    batch_id VARCHAR(50) NOT NULL,
    job_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    metric_name VARCHAR(255) NOT NULL,
    metric_value DECIMAL(18,2),
    threshold_value DECIMAL(18,2),
    status VARCHAR(20), -- PASS, FAIL, WARNING
    check_timestamp TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_data_quality_metrics_batch_id 
    ON data_quality_metrics(batch_id);

CREATE INDEX IF NOT EXISTS idx_data_quality_metrics_table_name 
    ON data_quality_metrics(table_name);

-- =====================================================
-- 5. JOB PARAMETERS TABLE
-- =====================================================
-- Stores job parameters and configuration
-- Equivalent to DataStage job parameters

CREATE TABLE IF NOT EXISTS job_parameters (
    parameter_id INTEGER AUTOINCREMENT PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL,
    parameter_name VARCHAR(255) NOT NULL,
    parameter_value VARCHAR(4000),
    parameter_type VARCHAR(50),
    description VARCHAR(1000),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (job_name, parameter_name)
);

-- Insert default parameters for RETAIL_DATA_MART_Job
INSERT INTO job_parameters (job_name, parameter_name, parameter_value, parameter_type, description)
VALUES 
    ('RETAIL_DATA_MART_Job', 'run_date', TO_CHAR(CURRENT_DATE(), 'YYYY-MM-DD'), 'DATE', 'Execution date for the job'),
    ('RETAIL_DATA_MART_Job', 'commit_batch', '1000', 'INTEGER', 'Batch size for commits'),
    ('RETAIL_DATA_MART_Job', 'log_path', '/tmp/dbt_logs', 'STRING', 'Path for log files'),
    ('RETAIL_DATA_MART_Job', 'source_connection', 'AVA_DB.PUBLIC', 'STRING', 'Source database connection'),
    ('RETAIL_DATA_MART_Job', 'target_connection', 'AVA_DB.PUBLIC', 'STRING', 'Target database connection')
ON CONFLICT (job_name, parameter_name) DO NOTHING;

-- =====================================================
-- 6. VIEWS FOR MONITORING
-- =====================================================

-- View: Recent Job Executions
CREATE OR REPLACE VIEW vw_recent_job_executions AS
SELECT 
    batch_id,
    job_name,
    start_time,
    end_time,
    DATEDIFF('second', start_time, end_time) AS duration_seconds,
    status,
    source_count,
    target_inserts,
    target_updates,
    error_message
FROM job_audit_log
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY start_time DESC;

-- View: Job Success Rate
CREATE OR REPLACE VIEW vw_job_success_rate AS
SELECT 
    job_name,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_runs,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs,
    ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS success_rate_pct
FROM job_audit_log
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY job_name;

-- View: Data Quality Summary
CREATE OR REPLACE VIEW vw_data_quality_summary AS
SELECT 
    table_name,
    metric_name,
    COUNT(*) AS total_checks,
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed_checks,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed_checks,
    SUM(CASE WHEN status = 'WARNING' THEN 1 ELSE 0 END) AS warning_checks,
    MAX(check_timestamp) AS last_check_time
FROM data_quality_metrics
WHERE check_timestamp >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY table_name, metric_name;

-- =====================================================
-- GRANT PERMISSIONS
-- =====================================================
-- Grant necessary permissions to roles
-- Adjust role names as per your Snowflake setup

GRANT SELECT, INSERT, UPDATE ON job_audit_log TO ROLE ACCOUNTADMIN;
GRANT SELECT, INSERT ON job_rejects TO ROLE ACCOUNTADMIN;
GRANT SELECT, INSERT ON dimension_audit TO ROLE ACCOUNTADMIN;
GRANT SELECT, INSERT ON data_quality_metrics TO ROLE ACCOUNTADMIN;
GRANT SELECT, INSERT, UPDATE ON job_parameters TO ROLE ACCOUNTADMIN;
GRANT SELECT ON vw_recent_job_executions TO ROLE ACCOUNTADMIN;
GRANT SELECT ON vw_job_success_rate TO ROLE ACCOUNTADMIN;
GRANT SELECT ON vw_data_quality_summary TO ROLE ACCOUNTADMIN;

-- =====================================================
-- COMPLETION MESSAGE
-- =====================================================
SELECT 'Audit framework setup completed successfully!' AS status;