{{ config({
    "materialized": "incremental",
    "unique_key": ["match_id", "inning", "batter"]
}) }}

WITH player_stats AS (
    SELECT
        match_id,
        inning,
        batting_team,
        bowling_team,
        batter,
        runs_batter,
        wide_runs,
        player_dismissed,
        wicket_type,
        bowler,
        fielders
    FROM {{ ref('stg_deliveries') }}
),

players AS (
    SELECT
        match_id,
        player_id,
        player_name
    FROM {{ ref('stg_playing_xi') }}
),

-- Get dismissal information for each player in a match
player_dismissed AS (
    SELECT DISTINCT -- Using DISTINCT instead of GROUP BY since all columns are needed
        match_id,
        inning,
        player_dismissed,
        wicket_type,
        bowler,
        fielders
    FROM player_stats
    WHERE player_dismissed IS NOT NULL
),

-- Calculate batting statistics for each player in a match
batting_stats AS (
    SELECT 
        match_id,
        inning,
        batting_team AS team,
        bowling_team AS opponent,
        batter,
        SUM(runs_batter) AS runs_scored,
        COUNT(*) - SUM(CASE WHEN wide_runs > 0 THEN 1 ELSE 0 END) AS balls_faced,
        COUNT(CASE WHEN runs_batter = 4 THEN 1 END) AS fours, -- Simplified CASE
        COUNT(CASE WHEN runs_batter = 6 THEN 1 END) AS sixes  -- Simplified CASE
    FROM player_stats
    GROUP BY 1, 2, 3, 4, 5
),

-- Combine batting stats with dismissal information
player_dismissed_stats AS (
    SELECT 
        b.*,
        COALESCE(pd.player_dismissed IS NOT NULL, FALSE) AS dismissed,
        COALESCE(pd.wicket_type, 'not out') AS wicket_type,
        pd.bowler,
        pd.fielders
    FROM batting_stats b
    LEFT JOIN player_dismissed pd ON 
        pd.match_id = b.match_id 
        AND pd.inning = b.inning
        AND pd.player_dismissed = b.batter
)

-- Final output with player ID and strike rate calculation
SELECT 
    p.player_id,
    pds.match_id,
    pds.inning,
    pds.team,
    pds.batter,
    pds.runs_scored,
    pds.balls_faced,
    pds.fours,
    pds.sixes,
    pds.dismissed,
    pds.wicket_type,
    pds.bowler,
    pds.fielders,
    CASE 
        WHEN pds.balls_faced > 0 THEN 
            ROUND((pds.runs_scored * 100.0 / pds.balls_faced), 2)
        ELSE NULL 
    END AS batting_strike_rate
FROM player_dismissed_stats pds
INNER JOIN players p ON 
    p.player_name = pds.batter 
    AND p.match_id = pds.match_id