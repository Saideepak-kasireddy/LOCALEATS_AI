-- Ensure all restaurant ratings are within valid range
SELECT
    restaurant_id,
    restaurant_name,
    yelp_rating
FROM {{ ref('dim_restaurants') }}
WHERE yelp_rating < 1.0 OR yelp_rating > 5.0