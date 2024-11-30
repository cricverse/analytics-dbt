{{ config({
    "materialized": "incremental",
    "unique_key": ["batter"]
}) }}

WITH player_stats AS (
    SELECT *
    FROM {{ ref('stg_deliveries') }}
),
players AS (
    SELECT *
    FROM {{ ref('stg_playing_xi') }}
),
batting_stats AS (
    SELECT 
        match_id,
        inning,
        batter,
    SUM(runs_batter) AS runs_scored,
    COUNT(*) AS balls_faced,
    SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) AS dismissed,
    COUNT(CASE WHEN runs_batter = 4 THEN 1 ELSE NULL END) AS fours,
    COUNT(CASE WHEN runs_batter = 6 THEN 1 ELSE NULL END) AS sixes
FROM player_stats
GROUP BY match_id, inning, batter
)

SELECT 
    players.player_id,
    batting_stats.*,
    CASE WHEN balls_faced > 0 THEN ROUND((runs_scored*100.0 / balls_faced), 2) ELSE NULL END AS batting_strike_rate
FROM batting_stats
JOIN players ON batting_stats.batter = players.player_name 
AND batting_stats.match_id = players.match_id