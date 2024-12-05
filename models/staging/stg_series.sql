{{ config({
    "materialized": "incremental",
    "unique_key": ["series_id"]
}) }}

WITH matches AS (
    SELECT * FROM {{ ref('stg_matches') }}
)

SELECT
    MD5(CONCAT(series_name, season)) AS series_id,
    series_name,
    season,
    ARRAY_AGG(DISTINCT format ORDER BY format) AS formats
FROM matches
GROUP BY series_name, season
