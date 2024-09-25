WITH raw_data AS (
    SELECT *
    FROM {{ source('raw', 'raw_matches') }}
)
SELECT 
    *
FROM raw_data