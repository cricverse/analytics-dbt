{{ config({
    "materialized": "incremental",
    "unique_key": ["match_id", "inning", "bowler"]
}) }}

WITH player_stats AS (
    SELECT *
    FROM {{ ref('stg_deliveries') }}
),
players AS (
    SELECT *
    FROM {{ ref('stg_playing_xi') }}
),
bowling_stats AS (
    SELECT 
        match_id,
        inning,
        bowler,
    SUM(runs_batter) AS runs_conceded,
    COUNT(*) AS balls_bowled,
    SUM(CASE WHEN wicket_type NOT IN ('run out', 'retired hurt', 'obstructing the field', 'handled the ball') THEN 1 ELSE 0 END) AS wickets_taken,
    COUNT(CASE WHEN runs_batter = 4 THEN 1 ELSE NULL END) AS fours_conceded,
    COUNT(CASE WHEN runs_batter = 6 THEN 1 ELSE NULL END) AS sixes_conceded,
    SUM(wide_runs) AS wides,
    SUM(noball_runs) AS no_balls
FROM player_stats
GROUP BY match_id, inning, bowler
)

SELECT
    players.player_id,
    bowling_stats.*,
    CASE WHEN balls_bowled > 0 THEN ROUND((runs_conceded*6.0 / balls_bowled), 2) ELSE NULL END AS economy,
    CASE WHEN wickets_taken > 0 THEN ROUND((balls_bowled*1.0 / wickets_taken), 2) ELSE NULL END AS bowling_strike_rate
FROM bowling_stats
JOIN players ON bowling_stats.bowler = players.player_name
AND bowling_stats.match_id = players.match_id