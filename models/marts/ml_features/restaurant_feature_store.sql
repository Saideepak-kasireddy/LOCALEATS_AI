{{
    config(
        materialized='table',
        post_hook="ALTER TABLE {{ this }} ADD CONSTRAINT pk_restaurant_features PRIMARY KEY (restaurant_id)"
    )
}}

WITH features AS (
    SELECT
        -- Identifiers
        r.restaurant_id,
        
        -- Numeric Features
        r.yelp_rating,
        LOG(r.yelp_review_count + 1) AS log_review_count,
        r.price_level_numeric,
        r.safety_score / 100.0 AS safety_score_normalized,
        r.accessibility_score / 100.0 AS accessibility_score_normalized,
        r.popularity_score / 100.0 AS popularity_score_normalized,
        r.value_score / 100.0 AS value_score_normalized,
        r.overall_recommendation_score / 100.0 AS overall_score_normalized,
        
        -- Location Features
        r.lat,
        r.lng,
        r.nearest_transit_meters / 1000.0 AS nearest_transit_km,
        r.transit_stops_within_1km,
        
        -- Temporal Features
        COALESCE(r.days_since_last_inspection / 365.0, 1.0) AS years_since_inspection,
        EXTRACT(MONTH FROM CURRENT_DATE()) AS current_month,
        EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) AS current_day_of_week,
        
        -- Categorical Encoded Features (One-hot encoding)
        IFF(r.neighborhood = 'Back Bay', 1, 0) AS neighborhood_back_bay,
        IFF(r.neighborhood = 'North End', 1, 0) AS neighborhood_north_end,
        IFF(r.neighborhood = 'South End', 1, 0) AS neighborhood_south_end,
        IFF(r.neighborhood = 'Cambridge', 1, 0) AS neighborhood_cambridge,
        IFF(r.neighborhood = 'Allston', 1, 0) AS neighborhood_allston,
        
        IFF(r.primary_cuisine_type ILIKE '%italian%', 1, 0) AS cuisine_italian,
        IFF(r.primary_cuisine_type ILIKE '%chinese%', 1, 0) AS cuisine_chinese,
        IFF(r.primary_cuisine_type ILIKE '%mexican%', 1, 0) AS cuisine_mexican,
        IFF(r.primary_cuisine_type ILIKE '%american%', 1, 0) AS cuisine_american,
        IFF(r.primary_cuisine_type ILIKE '%japanese%', 1, 0) AS cuisine_japanese,
        
        IFF(r.price_symbol = '$', 1, 0) AS price_tier_1,
        IFF(r.price_symbol = '$$', 1, 0) AS price_tier_2,
        IFF(r.price_symbol = '$$$', 1, 0) AS price_tier_3,
        IFF(r.price_symbol = '$$$$', 1, 0) AS price_tier_4,
        
        -- Risk Indicators
        IFF(r.health_risk_level = 'HIGH_RISK', 1, 0) AS is_high_risk,
        IFF(r.health_risk_level = 'LOW_RISK', 1, 0) AS is_low_risk,
        
        -- Target Variable (for training)
        IFF(f.city_rank <= 50, 1, 0) AS is_top_50,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS feature_extraction_timestamp
        
    FROM {{ ref('dim_restaurants') }} r
    LEFT JOIN {{ ref('fct_restaurant_recommendations') }} f
        ON r.restaurant_id = f.restaurant_id
        AND f.recommendation_date = CURRENT_DATE()
    WHERE r.is_permanently_closed = FALSE
)

SELECT * FROM features