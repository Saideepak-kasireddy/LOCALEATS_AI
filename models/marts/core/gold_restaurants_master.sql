-- models/marts/core/gold_restaurants_master.sql
{{
    config(
        materialized='table',
        tags=['gold']
    )
}}

WITH restaurants AS (
    SELECT * FROM {{ ref('stg_yelp_restaurants') }}
    WHERE is_closed = FALSE
),

scores AS (
    SELECT * FROM {{ ref('int_restaurant_scores') }}
),

inspections AS (
    SELECT * FROM {{ ref('int_inspection_aggregates') }}
),

transit AS (
    SELECT
        restaurant_id,
        nearest_stop_distance_m,
        nearest_stop_walk_time_min,
        nearest_stop_name,
        nearby_stops_count,
        accessible_stops_count
    FROM (
        SELECT
            restaurant_id,
            distance_meters as nearest_stop_distance_m,
            walking_time_minutes as nearest_stop_walk_time_min,
            stop_name as nearest_stop_name,
            COUNT(DISTINCT stop_id) OVER (PARTITION BY restaurant_id) as nearby_stops_count,
            COUNT(DISTINCT CASE WHEN is_wheelchair_accessible THEN stop_id END) OVER (PARTITION BY restaurant_id) as accessible_stops_count,
            ROW_NUMBER() OVER (PARTITION BY restaurant_id ORDER BY distance_meters) as rn
        FROM {{ ref('int_restaurant_transit_access') }}
        WHERE proximity_rank <= 10
    )
    WHERE rn = 1
),
google_places AS (
    SELECT * FROM {{ ref('stg_google_places') }}
),

-- Create rich text descriptions for embedding - now including Google Places attributes
restaurant_descriptions AS (
    SELECT
        r.restaurant_id,
        CONCAT(
            r.restaurant_name, '. ',
            'A ', COALESCE(r.primary_cuisine, 'restaurant'), ' restaurant ',
            'in ', r.neighborhood, ', ', r.city, '. ',
            CASE 
                WHEN r.price_tier IS NOT NULL THEN CONCAT('Price range: ', r.price_tier, '. ')
                ELSE ''
            END,
            'Rated ', r.yelp_rating, ' stars with ', r.yelp_review_count, ' reviews. ',
            'Categories: ', COALESCE(r.category_titles, 'General dining'), '. ',
            CASE 
                WHEN s.recommendation_tier = 'HIGHLY_RECOMMENDED' THEN 'Highly recommended. '
                WHEN s.recommendation_tier = 'RECOMMENDED' THEN 'Recommended. '
                ELSE ''
            END,
            CASE 
                WHEN i.health_risk_level = 'LOW_RISK' THEN 'Excellent safety record. '
                WHEN i.health_risk_level = 'MEDIUM_RISK' THEN 'Good safety record. '
                WHEN i.health_risk_level = 'HIGH_RISK' THEN 'Some safety concerns. '
                ELSE ''
            END,
            CASE 
                WHEN t.nearest_stop_distance_m <= 200 THEN 'Very close to public transit. '
                WHEN t.nearest_stop_distance_m <= 500 THEN 'Near public transit. '
                ELSE ''
            END,
            -- Add Google Places attributes to description
            CASE WHEN g.serves_vegetarian THEN 'Vegetarian-friendly. ' ELSE '' END,
            CASE WHEN g.reservable THEN 'Accepts reservations. ' ELSE '' END,
            CASE WHEN g.takeout THEN 'Takeout available. ' ELSE '' END,
            CASE WHEN g.delivery THEN 'Delivery available. ' ELSE '' END,
            CASE WHEN g.outdoor_seating THEN 'Outdoor seating. ' ELSE '' END,
            CASE WHEN g.good_for_groups THEN 'Good for groups. ' ELSE '' END,
            CASE WHEN g.is_wheelchair_accessible THEN 'Wheelchair accessible. ' ELSE '' END
        ) AS search_description
    FROM restaurants r
    LEFT JOIN scores s ON r.restaurant_id = s.restaurant_id
    LEFT JOIN inspections i ON r.restaurant_id = i.restaurant_id
    LEFT JOIN transit t ON r.restaurant_id = t.restaurant_id
    LEFT JOIN google_places g ON r.restaurant_id = g.restaurant_id
)

SELECT
    -- Restaurant Identity
    r.restaurant_id,
    r.restaurant_name,
    r.phone,
    r.url as yelp_url,
    
    -- Location
    r.street_address,
    r.city,
    r.state,
    r.postal_code,
    r.neighborhood,
    r.latitude,
    r.longitude,
    
    -- Cuisine & Categories
    r.primary_cuisine,
    r.category_titles,
    r.category_aliases,
    
    -- Yelp Metrics
    r.yelp_rating,
    r.yelp_review_count,
    r.price_tier,
    r.price_level,
    
    -- Scoring Dimensions
    s.safety_score,
    s.accessibility_score,
    s.popularity_score,
    s.value_score,
    s.overall_score,
    s.recommendation_tier,
    
    -- Safety Details
    i.total_inspections_all_time,
    i.pass_rate,
    i.recent_pass_rate,
    i.critical_violation_count,
    i.performance_category,
    i.health_risk_level,
    i.days_since_last_inspection,
    i.latest_inspection_date,
    i.total_violation_score,
    i.unique_violation_types,
    i.recent_inspection_count,
    
    -- Transit Details
    t.nearest_stop_name,
    t.nearest_stop_distance_m,
    t.nearest_stop_walk_time_min,
    t.nearby_stops_count,
    t.accessible_stops_count,
    
    -- Google Places Enrichment
    g.google_place_id,
    g.business_status,
    g.is_operational,
    
    -- Hours & Availability
    g.opening_hours_text,
    g.open_now,
    
    -- Dietary Options
    g.serves_vegetarian,
    g.serves_breakfast,
    g.serves_lunch,
    g.serves_dinner,
    g.serves_coffee,
    g.serves_dessert,
    g.serves_beer,
    g.serves_wine,
    
    -- Service Options
    COALESCE(g.dine_in, TRUE) as dine_in,  -- Default TRUE if unknown
    g.takeout,
    g.delivery,
    g.reservable,
    g.outdoor_seating,
    
    -- Accessibility Details
    g.wheelchair_accessible_entrance,
    g.wheelchair_accessible_parking,
    g.wheelchair_accessible_restroom,
    g.wheelchair_accessible_seating,
    g.is_wheelchair_accessible,
    
    -- Group & Family
    g.good_for_children,
    g.good_for_groups,
    g.allows_dogs,
    g.live_music,
    
    -- Combined Operational Status
    CASE 
        WHEN g.is_operational = FALSE THEN FALSE
        WHEN r.is_closed = TRUE THEN FALSE
        ELSE TRUE
    END as is_currently_open,
    
    -- Semantic Search Fields
    d.search_description,
    
    -- Metadata
    CURRENT_TIMESTAMP() as gold_created_at,
    g.enriched_at as google_enriched_at
    
FROM restaurants r
LEFT JOIN scores s ON r.restaurant_id = s.restaurant_id
LEFT JOIN inspections i ON r.restaurant_id = i.restaurant_id
LEFT JOIN transit t ON r.restaurant_id = t.restaurant_id
LEFT JOIN google_places g ON r.restaurant_id = g.restaurant_id
LEFT JOIN restaurant_descriptions d ON r.restaurant_id = d.restaurant_id