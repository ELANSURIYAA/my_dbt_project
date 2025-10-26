-- models/example_model.sql

ELECT
    current_date() AS run_date,
    'Hello from dbt Cloud!' AS message,
    1 AS example_value
