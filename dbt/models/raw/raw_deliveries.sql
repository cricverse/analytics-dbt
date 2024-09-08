WITH raw_data AS (
    SELECT *
    FROM {{ source('raw', 'raw_deliveries') }}
)
SELECT 
    *,
    
    (runs_off_bat + extras) AS runs_scored
FROM raw_data