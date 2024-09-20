WITH series_data AS (
    SELECT * FROM {{ ref('stg_series') }}
)

SELECT
    series_name,
    team_type AS series_type,
    season,
    ARRAY_AGG(DISTINCT team_name) AS teams,
    ARRAY_AGG(DISTINCT match_type) AS series_formats,
    COUNT(DISTINCT match_id) AS match_count
FROM
    series_data
GROUP BY 1, 2, 3