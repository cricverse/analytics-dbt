WITH raw_data AS (
    SELECT match_data
    FROM {{ ref('raw_matches') }}
),

players_data AS (
    SELECT
        team_name,
        player_name
    FROM raw_data,
         jsonb_each(match_data->'players') AS teams(team_name, players),
         jsonb_array_elements_text(players) AS player_name
),

registry_data AS (
    SELECT
        player_name,
        player_id
    FROM raw_data,
         jsonb_each_text(match_data->'registry'->'people') AS registry(player_name, player_id)
)

SELECT
    r.player_id,
    p.player_name,
    ARRAY_AGG(DISTINCT p.team_name) AS team_name
FROM players_data p
LEFT JOIN registry_data r
ON p.player_name = r.player_name
GROUP BY 1, 2
