WITH matches AS (
    SELECT * FROM {{ ref('stg_matches') }}
)

SELECT
    MD5(CONCAT(series_name, season, format)) AS series_id,
    series_name,
    season,
    format
FROM matches
GROUP BY series_name, season, format
