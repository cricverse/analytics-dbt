{{ config({
    "materialized": "table",
    "unique_key": ["match_id"]
}) }}

WITH raw_data AS (
    SELECT *
    FROM {{ source('raw', 'raw_matches') }}
)
SELECT 
    *
FROM raw_data