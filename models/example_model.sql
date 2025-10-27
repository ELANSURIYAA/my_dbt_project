-- models/example_model.sql

SELECT
    current_date() AS run_date,
    'Hello from dbt Cloud!' AS message,
    1 AS example_value
