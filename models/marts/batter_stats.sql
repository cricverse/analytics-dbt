WITH matches AS (
    SELECT *
    FROM {{ ref('stg_matches') }}
),
batting_match_stats AS (
    SELECT *
    FROM {{ ref('batting_match_stats') }}
),

batting_stats AS (
    SELECT batter,
        batting_match_stats.match_id,
        COUNT(inning) AS innings,
        SUM(runs_scored) AS runs_scored,
        SUM(balls_faced) AS balls_faced,
        SUM(dismissed) AS dismissals,
        SUM(fours) AS fours,
        SUM(sixes) AS sixes,
        SUM(
            CASE
                WHEN runs_scored = 0 THEN 1
                ELSE 0
            END
        ) AS ducks,
        SUM(
            CASE
                WHEN runs_scored >= 50 AND runs_scored < 100 THEN 1
                ELSE 0
            END
        ) AS fifties,
        SUM(
            CASE
                WHEN runs_scored >= 100 THEN 1
                ELSE 0
            END
        ) AS hundreds,
        MAX(runs_scored) AS highest_score,
        format,
        team_type
    FROM batting_match_stats
        JOIN matches ON batting_match_stats.match_id = matches.match_id
    GROUP BY batter,
        batting_match_stats.match_id,
        matches.format,
        matches.team_type
),

batting_summary AS (
    SELECT batter,
        format,
        team_type,
        COUNT(match_id) AS matches,
        SUM(innings) AS innings,
        SUM(innings) - SUM(dismissals) AS not_outs,
        SUM(runs_scored) AS runs,
        SUM(balls_faced) AS balls_faced,
        SUM(fours) AS fours,
        SUM(sixes) AS sixes,
        SUM(ducks) AS ducks,
        SUM(fifties) AS fifties,
        SUM(hundreds) AS hundreds,
        CASE
            WHEN SUM(dismissals) > 0 THEN ROUND(SUM(runs_scored) * 1.0 / SUM(dismissals), 2)
            ELSE NULL
        END AS average,
        CASE
            WHEN SUM(balls_faced) > 0 THEN ROUND(SUM(runs_scored) * 100.0 / SUM(balls_faced), 2)
            ELSE NULL
        END AS strike_rate,
        SUM(highest_score) AS highest_score
    FROM batting_stats
    GROUP BY batter,
        format,
        team_type
)

SELECT *
FROM batting_summary
