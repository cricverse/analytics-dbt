WITH raw_data AS (
    SELECT *
    FROM {{ source('raw', 'raw_matches') }}
)
SELECT 
    match_id,
    match_data,
    deliveries
FROM raw_data