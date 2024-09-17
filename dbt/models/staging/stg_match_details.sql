WITH raw_data AS (
    SELECT 
        match_id, 
        match_data
    FROM {{ ref('raw_matches') }}
)

SELECT
    match_id,
    match_data->'event'->>'name' AS series_name,
    match_data->'event'->>'match_number' AS match_num,
    match_data->'event'->>'stage' AS match_stage,
    match_data->'match_type' AS match_type,
    match_data->'match_type_number' AS match_type_num,
    match_data->'season' AS season,
    match_data->'player_of_match' AS player_of_match,
    match_data->'dates' AS match_dates,
    match_data->'venue' AS venue,
    match_data->'toss'->>'winner' AS toss_winner,
    match_data->'toss'->>'decision' AS toss_decision,
    match_data->'outcome'->>'result' AS outcome_type,
    match_data->'teams'->0 AS team1,
    match_data->'teams'->1 AS team2,
    -- -- Handling winner, win_type, and win_by
    COALESCE(match_data->'outcome'->>'winner', match_data->'outcome'->>'eliminator') AS winner,

   match_data->'outcome'->>'by' AS win_by

FROM raw_data
