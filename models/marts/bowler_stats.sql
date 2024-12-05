WITH matches AS (
    SELECT *
    FROM {{ ref('stg_matches') }}
),
bowling_match_stats AS (
    SELECT *
    FROM {{ ref('bowling_match_stats') }}
),
best_bowling_innings AS (
    SELECT DISTINCT ON (bowler) 
        bowler,
        match_id,
        wickets_taken AS taken,
        runs_conceded AS conceded
    FROM bowling_match_stats
    ORDER BY bowler, wickets_taken DESC, runs_conceded ASC
),
best_bowling_match AS (
    SELECT DISTINCT ON (bowler, match_id) 
        bowler,
        match_id,
        wickets_taken AS taken,
        runs_conceded AS conceded
    FROM bowling_match_stats
    ORDER BY bowler, match_id, taken DESC, conceded ASC
),
bowling_match_summary AS (
    SELECT bowling_match_stats.bowler,
        bowling_match_stats.match_id,
        COUNT(inning) AS innings,
        SUM(runs_conceded) AS runs_conceded,
        SUM(balls_bowled) AS balls_bowled,
        SUM(wickets_taken) AS wickets_taken,
        SUM(maidens) AS maidens,
        matches.format,
        matches.team_type,
        MAX(best_bowling_innings.taken) AS most_wkts,
        MIN(best_bowling_innings.conceded) AS min_runs,
        COUNT(CASE WHEN wickets_taken = 4 THEN 1 END) AS "4ws",
        COUNT(CASE WHEN wickets_taken = 5 THEN 1 END) AS "5ws"
    FROM bowling_match_stats
        JOIN matches ON bowling_match_stats.match_id = matches.match_id
        JOIN best_bowling_innings ON bowling_match_stats.bowler = best_bowling_innings.bowler
    GROUP BY 
        bowling_match_stats.bowler,
        bowling_match_stats.match_id,
        matches.format,
        matches.team_type
)

SELECT 
    bowler,
    COUNT(match_id) AS matches,
    SUM(innings) AS innings,
    SUM(runs_conceded) AS runs_conceded,
    SUM(balls_bowled) AS balls_bowled,
    SUM(wickets_taken) AS wickets_taken,
    SUM(maidens) AS maidens,
    CASE WHEN SUM(balls_bowled) > 0 THEN ROUND(SUM(runs_conceded) * 6.0 / SUM(balls_bowled), 2) END AS economy_rate,
    CASE WHEN SUM(wickets_taken) > 0 THEN ROUND(SUM(runs_conceded) * 1.0 / SUM(wickets_taken), 2) END AS average,
    CASE WHEN SUM(wickets_taken) > 0 THEN ROUND(SUM(balls_bowled) * 1.0 / SUM(wickets_taken), 2) END AS strike_rate,
    CONCAT(MAX(most_wkts), '/', MIN(min_runs)) AS bbi,
    CONCAT(MAX(wickets_taken), '/', MIN(runs_conceded)) AS bbm,
    SUM("4ws") AS "4ws",
    SUM("5ws") AS "5ws",
    COUNT(CASE WHEN wickets_taken = 10 THEN 1 END) AS "10ws"
FROM bowling_match_summary
GROUP BY bowler