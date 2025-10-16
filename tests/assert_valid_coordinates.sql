-- Ensure all coordinates are within Boston area bounds
SELECT
    restaurant_id,
    restaurant_name,
    lat,
    lng
FROM {{ ref('dim_restaurants') }}
WHERE 
    lat NOT BETWEEN 42.2 AND 42.5
    OR lng NOT BETWEEN -71.2 AND -70.9