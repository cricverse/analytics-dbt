{{ config({
    "materialized": "incremental",
    "unique_key": ["match_id", "inning", "batter"]
}) }}

WITH player_stats AS (
    SELECT *
    FROM {{ ref('stg_deliveries') }}
),
players AS (
    SELECT *
    FROM {{ ref('stg_playing_xi') }}
),
player_dismissed AS (
    SELECT 
        match_id,
        inning,
        player_dismissed
    FROM player_stats
    GROUP BY match_id, inning, player_dismissed
),
batting_stats AS (
    SELECT 
        match_id,
        inning,
        batter,
        SUM(runs_batter) AS runs_scored,
        COUNT(*) - SUM(CASE WHEN wide_runs > 0 THEN 1 ELSE 0 END) AS balls_faced,
        COUNT(CASE WHEN runs_batter = 4 THEN 1 ELSE NULL END) AS fours,
        COUNT(CASE WHEN runs_batter = 6 THEN 1 ELSE NULL END) AS sixes
FROM player_stats
GROUP BY match_id, inning, batter
),
player_dismissed_stats AS (
SELECT 
    batting_stats.*,
    CASE WHEN player_dismissed.player_dismissed IS NOT NULL THEN 1 ELSE 0 END AS dismissed
FROM player_dismissed
RIGHT JOIN batting_stats ON player_dismissed.match_id = batting_stats.match_id 
AND player_dismissed.inning = batting_stats.inning 
AND player_dismissed.player_dismissed = batting_stats.batter
)
SELECT 
    players.player_id,
    player_dismissed_stats.*,
    CASE WHEN balls_faced > 0 THEN ROUND((runs_scored*100.0 / balls_faced), 2) ELSE NULL END AS batting_strike_rate
FROM player_dismissed_stats
JOIN players ON player_dismissed_stats.batter = players.player_name 
AND player_dismissed_stats.match_id = players.match_id