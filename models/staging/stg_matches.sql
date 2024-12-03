{{ config({
    "materialized": "incremental",
    "unique_key": ["match_id"]
}) }}

WITH raw_data AS (
    SELECT 
        match_id, 
        match_data
    FROM {{ ref('raw_data') }}
)

SELECT
    match_id,
    match_data->'event'->>'name' AS series_name,
    match_data->'event'->>'match_number' AS match_num,
    match_data->'event'->>'stage' AS match_stage,
    match_data->>'match_type_number' AS match_type_num,
    match_data->>'team_type' AS team_type,
    match_data->>'match_type' AS format,
    match_data->>'season' AS season,
    match_data->>'player_of_match' AS player_of_match,
    match_data->>'dates' AS match_dates,
    JSONB_ARRAY_LENGTH(match_data->'dates') AS num_days,
    match_data->>'venue' AS venue,
    match_data->'toss'->>'winner' AS toss_winner,
    match_data->'toss'->>'decision' AS toss_decision,
    COALESCE(match_data->'outcome'->>'result', 'win') AS outcome,
    match_data->'teams'->>0 AS team1,
    match_data->'teams'->>1 AS team2,
    COALESCE(match_data->'outcome'->>'winner', match_data->'outcome'->>'eliminator') AS winner,
    (match_data->'outcome'->'by'->>'wickets')::int AS win_by_wickets,
    (match_data->'outcome'->'by'->>'runs')::int AS win_by_runs,
    (match_data->'outcome'->'by'->>'innings')::int AS win_by_innings

FROM raw_data
