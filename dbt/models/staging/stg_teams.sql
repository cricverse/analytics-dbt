WITH raw_data AS (
    SELECT
        match_id, 
        match_data
    FROM {{ ref('raw_matches') }}
),

raw_teams AS (
SELECT
    UNNEST(ARRAY[match_data->'teams'->>0, match_data->'teams'->>1]) AS team_name,
    match_data->>'team_type' AS team_type
FROM raw_data
)

SELECT
    DISTINCT team_name,
    {{ dbt_utils.generate_surrogate_key(['team_name']) }} AS team_id,
    team_type
FROM raw_teams