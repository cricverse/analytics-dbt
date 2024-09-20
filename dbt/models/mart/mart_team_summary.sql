WITH match_details AS (
    SELECT * FROM {{ ref('stg_match_details') }}
),
teams AS (
    SELECT * FROM {{ ref('stg_teams') }}
)

SELECT 
    team_name,
    match_type,
    match_id,
    winner,
    series_name,
    match_num,
    match_stage,
    season

FROM teams
LEFT JOIN match_details
ON teams.team_name = match_details.team1
OR teams.team_name = match_details.team2
