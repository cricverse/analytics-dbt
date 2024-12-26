{% macro get_match_format(format, team_type) %}
CASE 
WHEN format = 'T20' AND team_type = 'club' THEN 'T20'
WHEN format = 'T20' AND team_type = 'international' THEN 'T20I'
WHEN format = 'MDM' THEN 'First Class'
WHEN format = 'ODM' THEN 'List A'
WHEN format = 'ODI' THEN 'ODI'
WHEN format = 'Test' THEN 'Test'
ELSE 'Unknown'
END
{% endmacro %}