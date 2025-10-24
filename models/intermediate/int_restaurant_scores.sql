-- models/intermediate/int_restaurant_scores.sql
{{
    config(
        materialized='table'
    )
}}

WITH restaurants AS (
    SELECT * FROM {{ ref('stg_yelp_restaurants') }}
    WHERE is_closed = FALSE
),

inspection_agg AS (
    SELECT * FROM {{ ref('int_inspection_aggregates') }}
),

transit_metrics AS (
    SELECT
        restaurant_id,
        COUNT(DISTINCT stop_id) AS nearby_stops_count,
        MIN(distance_meters) AS nearest_stop_distance,
        MIN(walking_time_minutes) AS nearest_stop_walk_time,
        COUNT(DISTINCT CASE WHEN is_wheelchair_accessible THEN stop_id END) AS accessible_stops_count,
        COUNT(DISTINCT CASE WHEN accessibility_category IN ('IMMEDIATE', 'VERY_CLOSE') THEN stop_id END) AS very_close_stops
    FROM {{ ref('int_restaurant_transit_access') }}
    WHERE proximity_rank <= 10
    GROUP BY restaurant_id
),

scoring AS (
    SELECT
        r.restaurant_id,
        r.restaurant_name,
        r.yelp_rating,
        r.yelp_review_count,
        r.price_level,
        
        -- Safety Score (0-100) based on inspections
        CASE
            WHEN i.total_inspections_all_time = 0 OR i.total_inspections_all_time IS NULL THEN 50
            WHEN i.days_since_last_inspection > 365 THEN 
                GREATEST(0, 40 - (i.days_since_last_inspection - 365) / 10)
            ELSE
                GREATEST(0, LEAST(100,
                    70 + 
                    (i.pass_rate * 0.3) -
                    (i.critical_violation_count * 5) -
                    (i.total_violation_score * 0.1)
                ))
        END AS safety_score,
        
        -- Accessibility Score (0-100) based on transit
        CASE
            WHEN t.nearby_stops_count = 0 OR t.nearby_stops_count IS NULL THEN 0
            ELSE GREATEST(0, LEAST(100,
                CASE
                    WHEN t.nearest_stop_distance <= 200 THEN 100
                    WHEN t.nearest_stop_distance <= 500 THEN 85
                    WHEN t.nearest_stop_distance <= 1000 THEN 65
                    ELSE 40
                END * 0.6 +
                (t.very_close_stops * 4) * 0.4
            ))
        END AS accessibility_score,
        
        -- Popularity Score (0-100) based on Yelp
        CASE
            WHEN r.yelp_review_count = 0 THEN 0
            ELSE GREATEST(0, LEAST(100,
                (r.yelp_rating / 5.0 * 100) * 0.6 +
                (CASE
                    WHEN r.yelp_review_count >= 500 THEN 100
                    WHEN r.yelp_review_count >= 200 THEN 80
                    WHEN r.yelp_review_count >= 100 THEN 60
                    ELSE r.yelp_review_count * 0.6
                END) * 0.4
            ))
        END AS popularity_score,
        
        -- Value Score (0-100) based on price/quality ratio
        CASE
            WHEN r.price_level = 0 THEN 50
            ELSE GREATEST(0, LEAST(100,
                ((5 - r.price_level) * 20) * 0.5 +
                (r.yelp_rating * 20 / GREATEST(r.price_level, 1)) * 0.5
            ))
        END AS value_score,
        
        -- Include useful metadata
        i.total_inspections_all_time,
        i.latest_inspection_date,
        i.days_since_last_inspection,
        i.health_risk_level,
        t.nearest_stop_distance,
        t.nearby_stops_count
        
    FROM restaurants r
    LEFT JOIN inspection_agg i ON r.restaurant_id = i.restaurant_id
    LEFT JOIN transit_metrics t ON r.restaurant_id = t.restaurant_id
)

SELECT
    *,
    -- Overall score (weighted average)
    ROUND(
        COALESCE(safety_score, 50) * 0.35 +
        COALESCE(accessibility_score, 50) * 0.15 +
        COALESCE(popularity_score, 50) * 0.35 +
        COALESCE(value_score, 50) * 0.15,
        2
    ) AS overall_score,
    
    -- Recommendation tier
    CASE
        WHEN (COALESCE(safety_score, 50) * 0.35 + COALESCE(accessibility_score, 50) * 0.15 + 
              COALESCE(popularity_score, 50) * 0.35 + COALESCE(value_score, 50) * 0.15) >= 80 THEN 'HIGHLY_RECOMMENDED'
        WHEN (COALESCE(safety_score, 50) * 0.35 + COALESCE(accessibility_score, 50) * 0.15 + 
              COALESCE(popularity_score, 50) * 0.35 + COALESCE(value_score, 50) * 0.15) >= 60 THEN 'RECOMMENDED'
        WHEN (COALESCE(safety_score, 50) * 0.35 + COALESCE(accessibility_score, 50) * 0.15 + 
              COALESCE(popularity_score, 50) * 0.35 + COALESCE(value_score, 50) * 0.15) >= 40 THEN 'AVERAGE'
        ELSE 'BELOW_AVERAGE'
    END AS recommendation_tier,
    
    CURRENT_TIMESTAMP() AS score_calculated_at
    
FROM scoring