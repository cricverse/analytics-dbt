WITH players AS (
    SELECT * FROM {{ ref('stg_players') }}
)

SELECT
    player_name,
    COUNT(match_id) AS total_matches,
    ARRAY_AGG(DISTINCT team_name) AS teams_played_for
FROM players
GROUP BY player_name
