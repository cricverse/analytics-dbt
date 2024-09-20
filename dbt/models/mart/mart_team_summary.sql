WITH match_details AS (
    SELECT * FROM {{ ref('stg_match_details') }}
),
teams AS (
    SELECT * FROM {{ ref('stg_teams') }}
)

SELECT 
    DISTINCT team_name,
    matches_played,
    series_played,
    matches_won
FROM (
    SELECT 
        team_name,
        match_type,
        ARRAY_AGG(DISTINCT match_id) AS matches_played,
        ARRAY_AGG(DISTINCT series_name) AS series_played,
        CASE WHEN winner = team_name THEN ARRAY_AGG(DISTINCT match_id) ELSE NULL END AS matches_won
    FROM teams
    LEFT JOIN match_details
    ON teams.team_name = match_details.team1
    OR teams.team_name = match_details.team2

    GROUP BY team_name, match_type, winner
) AS team_summary