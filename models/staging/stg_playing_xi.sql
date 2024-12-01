{{ config({
    "materialized": "incremental",
    "unique_key": ["match_id", "player_id"]
}) }}

WITH raw_data AS (
    SELECT match_id, match_data
    FROM {{ ref('raw_matches') }}
),

players AS (
    SELECT
        match_id,
        team_name,
        JSONB_ARRAY_ELEMENTS_TEXT(players) AS player_name
    FROM 
        raw_data,
        JSONB_EACH(match_data->'players') AS teams(team_name, players)
),

registry AS (
    SELECT
        match_id,
        player_name,
        player_id
    FROM
        raw_data,
        JSONB_EACH_TEXT(match_data->'registry'->'people') AS registry(player_name, player_id)
)

SELECT
    players.match_id,
    registry.player_id,
    players.player_name,
    players.team_name
FROM players
LEFT JOIN registry
ON players.match_id = registry.match_id
AND players.player_name = registry.player_name