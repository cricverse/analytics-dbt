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
),

player_teams AS (
    SELECT
        r.player_id,
        p.player_name,
        p.team_name,
        t.team_type
    FROM players_data p
    LEFT JOIN registry_data r
    ON p.player_name = r.player_name
    JOIN {{ ref('stg_teams') }} t
    ON p.team_name = t.team_name
)

SELECT
    player_id,
    player_name,
    team_name,
    team_type
FROM player_teams
GROUP BY 1, 2, 3, 4