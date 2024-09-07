{{ config(
        materialized = 'incremental',
        unique_key = 'player_id'
    )
}}

WITH distinct_players AS (
    SELECT DISTINCT
        striker AS player_name
    FROM {{ source('raw', 'raw_deliveries') }}
    UNION
    SELECT DISTINCT
        non_striker AS player_name
    FROM {{ source('raw', 'raw_deliveries') }}
    UNION
    SELECT DISTINCT
        bowler AS player_name
    FROM {{ source('raw', 'raw_deliveries') }}
)

SELECT
    player_name,
    substring(md5(player_name), 1, 8) AS player_id
FROM distinct_players

