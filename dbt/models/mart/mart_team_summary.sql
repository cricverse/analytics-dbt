WITH match_details AS (
    SELECT * FROM {{ ref('stg_match_details') }}
),
teams AS (
    SELECT * FROM {{ ref('stg_teams') }}
)

SELECT 
    team_name,
    match_type,
    COUNT(teams.match_id) AS total_matches,
    ARRAY_AGG(DISTINCT teams.series_name) AS series_played,
    COUNT(DISTINCT CASE WHEN winner = team_name THEN TEAMS.match_id ELSE NULL END) AS total_wins,
    COUNT(DISTINCT CASE WHEN outcome_type = 'draw' THEN TEAMS.match_id ELSE NULL END) AS draws

FROM teams
LEFT JOIN match_details
ON teams.match_id = match_details.match_id

GROUP BY team_name, match_type
ORDER BY total_matches DESC