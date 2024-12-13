{{ config({
    "materialized": "incremental",
    "unique_key": ["match_id", "inning", "over", "ball"]
}) }}

WITH 
    delivery_data AS (
        SELECT 
            match_id,
            jsonb_array_elements(deliveries) AS delivery
        FROM {{ ref('raw_data') }}
    ),

    teams AS (
    SELECT 
        match_id,
        ARRAY_AGG(DISTINCT delivery->>'team' ORDER BY delivery->>'team') AS teams
    FROM 
        delivery_data
    GROUP BY match_id
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
            (overs->>'over')::int AS over,
            jsonb_array_elements(overs->'deliveries') AS deliveries
        FROM innings_data
    ),
    
    deliveries_data AS (
        SELECT 
            overs_data.match_id,
            inning,
            over,
            batting_team,
            (SELECT team FROM unnest(teams) AS team WHERE team != batting_team) AS bowling_team,
            row_number() over(PARTITION BY overs_data.match_id, inning, over) AS ball,
            deliveries->>'batter' AS batter,
            deliveries->>'bowler' AS bowler,
            (deliveries->'runs'->>'total')::int AS runs_total,
            (deliveries->'runs'->>'batter')::int AS runs_batter,
            (deliveries->'runs'->>'extras')::int AS runs_extras,
            deliveries->>'non_striker' AS non_striker,
            deliveries->'wickets'->0 IS NOT NULL AS is_wicket,
            deliveries->'wickets'->0->>'kind' AS wicket_type,
            deliveries->'wickets'->0->>'player_out' AS player_dismissed,
            deliveries->'wickets'->0->'fielders' AS fielders_list,
            COALESCE((deliveries->'extras'->>'wides')::int, 0) AS wide_runs,
            COALESCE((deliveries->'extras'->>'noballs')::int, 0) AS noball_runs,
            COALESCE((deliveries->'extras'->>'byes')::int, 0) AS bye_runs,
            COALESCE((deliveries->'extras'->>'legbyes')::int, 0) AS legbye_runs,
            COALESCE((deliveries->'extras'->>'penalty')::int, 0) AS penalty_runs
        FROM overs_data
        JOIN teams
        ON overs_data.match_id = teams.match_id
    ),
    
    fielders_data AS (
        SELECT 
            match_id,
            inning,
            batting_team,
            over,
            ball,
            array_agg(jsonb_extract_path_text(value, 'name')) AS fielders
        FROM deliveries_data,
        LATERAL jsonb_array_elements(fielders_list) AS value
        WHERE fielders_list IS NOT NULL
        GROUP BY match_id, inning, batting_team, over, ball
    )

SELECT 
    d.match_id,
    d.inning,
    d.batting_team,
    d.bowling_team,
    d.over,
    d.ball,
    d.batter,
    d.bowler,
    d.non_striker,
    d.runs_total,
    d.runs_batter,
    d.runs_extras,
    d.is_wicket,
    d.wicket_type,
    d.player_dismissed,
    f.fielders,
    d.wide_runs,
    d.noball_runs,
    d.bye_runs,
    d.legbye_runs
FROM deliveries_data d
LEFT JOIN fielders_data f
ON d.match_id = f.match_id
AND d.inning = f.inning
AND d.over = f.over
AND d.ball = f.ball
ORDER BY d.match_id, d.inning, d.over, d.ball