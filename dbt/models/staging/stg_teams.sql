WITH raw_data AS (
    SELECT
        match_id, 
        match_data
    FROM {{ ref('raw_matches') }}
),

raw_teams AS (
SELECT
    UNNEST(ARRAY[match_data->'teams'->>0, match_data->'teams'->>1]) AS team_name,
    UNNEST(ARRAY[match_data->'teams'->>1, match_data->'teams'->>0]) AS opponent,
    match_data->'outcome'->>'winner' AS winner,
    match_data->'outcome'->>'result' AS outcome_type
FROM raw_data
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['team_name']) }} AS team_id,
    team_name,
    {{ dbt_utils.generate_surrogate_key(['opponent']) }} AS
    opponent,
    COUNT(team_name) AS total_matches,
    COUNT(CASE WHEN team_name = winner THEN 1 ELSE NULL END) AS total_wins,
    COUNT(CASE WHEN outcome_type = 'draw' THEN 1 ELSE NULL END) AS draws
FROM raw_teams
GROUP BY team_name, opponent
