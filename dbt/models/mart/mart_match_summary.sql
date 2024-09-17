WITH match_details AS (
    SELECT * FROM {{ ref('stg_match_details') }}
),
teams AS (
    SELECT * FROM {{ ref('stg_teams') }}
)

SELECT
    md.match_id,
    md.match_type,
    md.venue,
    t1.team_name AS team1,
    t2.team_name AS team2,
    md.winner,
    md.win_type,
    md.win_by,
    md.toss_decision,
    md.toss_winner,
    md.match_dates
FROM match_details md

JOIN teams t1 ON t1.match_id = md.match_id
JOIN teams t2 ON t2.match_id = md.match_id
WHERE t1.team_name < t2.team_name
