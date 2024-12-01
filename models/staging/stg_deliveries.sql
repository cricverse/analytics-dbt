{{ config({
    "materialized": "incremental",
    "unique_key": ["match_id", "inning", "over", "ball"]
}) }}

WITH delivery_data AS (
    SELECT
        match_id,
        jsonb_array_elements(deliveries) AS delivery
    FROM {{ ref('raw_matches') }}
),

innings_data AS (
    SELECT
        match_id,
        ROW_NUMBER() OVER(PARTITION BY match_id) AS inning,
        delivery->>'team' AS batting_team,
        jsonb_array_elements(delivery->'overs') AS overs
    FROM delivery_data
    GROUP BY match_id, delivery
),

overs_data AS (
    SELECT 
        match_id,
        inning,
        batting_team,
        overs->>'over' AS over,
        jsonb_array_elements(overs->'deliveries') AS deliveries
    FROM innings_data
)

SELECT 
    match_id,
    inning,
    batting_team,
    over,
    row_number() over(PARTITION BY match_id, inning, over) AS ball,
    deliveries->>'batter' AS batter,
    deliveries->>'bowler' AS bowler,
    (deliveries->'runs'->>'total')::int AS runs_total,
    (deliveries->'runs'->>'batter')::int AS runs_batter,
    (deliveries->'runs'->>'extras')::int AS runs_extras,
    deliveries->>'non_striker' AS non_striker,

    CASE WHEN deliveries->'wickets'->0 IS NOT NULL THEN TRUE ELSE FALSE END AS is_wicket,
    deliveries->'wickets'->0->>'kind' AS wicket_type,
    deliveries->'wickets'->0->>'player_out' AS player_dismissed,
    deliveries->'wickets'->0->'fielders' AS fielders,

    COALESCE(deliveries->'extras'->>'wides', '0')::int AS wide_runs,
    COALESCE(deliveries->'extras'->>'noballs', '0')::int AS noball_runs,
    COALESCE(deliveries->'extras'->>'byes', '0')::int AS bye_runs,
    COALESCE(deliveries->'extras'->>'legbyes', '0')::int AS legbye_runs

FROM overs_data
ORDER BY over DESC