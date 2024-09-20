WITH raw_data AS (
    SELECT match_id, match_data
    FROM {{ ref('raw_matches') }}
),

players AS
(
    SELECT
        match_id,
        team_name,
        UNNEST(ARRAY[jsonb_array_elements_text(players)]) AS player_name
    FROM 
        raw_data,
        jsonb_each(match_data->'players') AS teams(team_name, players)
),

registry AS
(
    SELECT
        match_id,
        player_name,
        player_id
    FROM
        raw_data,
        jsonb_each_text(match_data->'registry'->'people') AS registry(player_name, player_id)
)

SELECT
    players.match_id,
    team_name,
    player_id
FROM players
LEFT JOIN registry
ON players.match_id = registry.match_id
AND players.player_name = registry.player_name