WITH raw_data AS (
    SELECT *
    FROM {{ source('raw', 'raw_match_info') }}
)
SELECT 
    *
FROM raw_data