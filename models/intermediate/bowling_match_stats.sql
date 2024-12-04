{{ config({
    "materialized": "incremental",
    "unique_key": ["match_id", "inning", "bowler"]
}) }}

WITH deliveries AS (
    SELECT *
    FROM {{ ref('stg_deliveries') }}
),
players AS (
    SELECT *
    FROM {{ ref('stg_playing_xi') }}
),
dismissals AS (
    SELECT *
    FROM {{ ref('dismissals') }}
),
bowling_stats AS (
    SELECT
        match_id,
        inning,
        bowler,
        SUM(runs_batter + wide_runs + noball_runs) AS runs_conceded,
        COUNT(*) - COUNT(CASE WHEN wide_runs > 0 THEN 1 END) - COUNT(CASE WHEN noball_runs > 0 THEN 1 END) AS balls_bowled,
        SUM(CASE WHEN wicket_type IS NOT NULL 
            AND d.is_bowler_wicket = true THEN 1 ELSE 0 END) AS wickets_taken,
        COUNT(CASE WHEN runs_batter = 4 THEN 1 END) AS fours_conceded,
        COUNT(CASE WHEN runs_batter = 6 THEN 1 END) AS sixes_conceded,
        SUM(wide_runs) AS wide_runs,
        SUM(noball_runs) AS noball_runs,
        COUNT(CASE WHEN wide_runs > 0 THEN 1 END) AS wide_balls,
        COUNT(CASE WHEN noball_runs > 0 THEN 1 END) AS noballs,
        COUNT(CASE WHEN wide_runs = 5 THEN 1 END) AS five_wides,
        COUNT(CASE WHEN runs_batter + wide_runs + noball_runs = 0 THEN 1 END) AS dot_balls
    FROM deliveries
    LEFT JOIN dismissals d 
        ON d.dismissal_type = deliveries.wicket_type
    GROUP BY match_id, inning, bowler
),
maiden_stats AS (
    SELECT 
        match_id,
        inning,
        bowler,
        COUNT(DISTINCT over) AS maidens
    FROM (
        SELECT 
            match_id,
            inning,
            bowler,
            over,
            SUM(runs_batter + wide_runs + noball_runs) AS over_runs
        FROM deliveries
        GROUP BY match_id, inning, bowler, over
        HAVING SUM(runs_batter + wide_runs + noball_runs) = 0
    ) maiden_overs
    GROUP BY match_id, inning, bowler
)

SELECT
    players.player_id,
    bowling_stats.*,
    COALESCE(maiden_stats.maidens, 0) AS maidens,
    ROUND(NULLIF(runs_conceded * 6.0 / NULLIF(balls_bowled, 0), 0), 2) AS economy,
    ROUND(NULLIF(balls_bowled * 1.0 / NULLIF(wickets_taken, 0), 0), 2) AS bowling_strike_rate
FROM bowling_stats
LEFT JOIN maiden_stats 
    ON bowling_stats.match_id = maiden_stats.match_id
    AND bowling_stats.inning = maiden_stats.inning
    AND bowling_stats.bowler = maiden_stats.bowler
JOIN players 
    ON bowling_stats.bowler = players.player_name
    AND bowling_stats.match_id = players.match_id