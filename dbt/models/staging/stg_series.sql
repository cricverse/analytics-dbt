WITH raw_data AS
(
    SELECT
        match_id,
        match_data
    FROM {{ ref('raw_matches') }}
),

raw_series AS (
    SELECT 
        match_data->'event'->>'name' AS series_name,
        match_id,
        UNNEST(ARRAY[match_data->'teams'->>0, match_data->'teams'->>1]) AS team_name,
        match_data->'team_type' AS team_type,
        match_data->'match_type' AS match_type,
        match_data->'season' AS season
    FROM 
        raw_data
)

SELECT
    DISTINCT series_name,
    match_id,
    team_name,
    team_type,
    match_type,
    season
FROM
    raw_series
