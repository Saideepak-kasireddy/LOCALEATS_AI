{% macro haversine_distance(lat1, lon1, lat2, lon2) %}
    6371000 * ACOS(
        LEAST(1.0,
            COS(RADIANS({{ lat1 }})) * 
            COS(RADIANS({{ lat2 }})) * 
            COS(RADIANS({{ lon2 }}) - RADIANS({{ lon1 }})) +
            SIN(RADIANS({{ lat1 }})) * 
            SIN(RADIANS({{ lat2 }}))
        )
    )
{% endmacro %}