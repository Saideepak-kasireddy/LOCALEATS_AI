{{
    config(
        materialized='table'
    )
}}

WITH restaurants AS (
    SELECT * FROM {{ ref('stg_yelp_restaurants') }}
),

-- Aggregate inspection metrics
inspection_metrics AS (
    SELECT
        restaurant_id,
        COUNT(DISTINCT inspection_id) AS total_inspections,
        COUNT(DISTINCT CASE WHEN inspection_result = 'PASS' THEN inspection_id END) AS passed_inspections,
        COUNT(DISTINCT CASE WHEN inspection_result = 'FAIL' THEN inspection_id END) AS failed_inspections,
        COUNT(DISTINCT CASE WHEN violation_severity != 'NONE' THEN inspection_id END) AS inspections_with_violations,
        SUM(violation_severity_score) AS total_violation_points,
        AVG(violation_severity_score) AS avg_violation_score,
        MAX(inspection_date) AS latest_inspection_date,
        MIN(days_since_inspection) AS days_since_last_inspection,
        
        -- Recent inspection metrics (last 6 months)
        COUNT(DISTINCT CASE 
            WHEN days_since_inspection <= 180 AND inspection_result = 'PASS' 
            THEN inspection_id 
        END) AS recent_passes,
        COUNT(DISTINCT CASE 
            WHEN days_since_inspection <= 180 AND inspection_result = 'FAIL' 
            THEN inspection_id 
        END) AS recent_fails
        
    FROM {{ ref('int_restaurant_inspections') }}
    GROUP BY restaurant_id
),

-- Aggregate transit metrics
transit_metrics AS (
    SELECT
        restaurant_id,
        COUNT(DISTINCT stop_id) AS nearby_stops_count,
        MIN(distance_meters) AS nearest_stop_distance,
        MIN(walking_time_minutes) AS nearest_stop_walk_time,
        COUNT(DISTINCT CASE WHEN is_wheelchair_accessible THEN stop_id END) AS accessible_stops_count,
        COUNT(DISTINCT CASE WHEN accessibility_category = 'IMMEDIATE' THEN stop_id END) AS immediate_stops,
        COUNT(DISTINCT CASE WHEN accessibility_category IN ('IMMEDIATE', 'VERY_CLOSE') THEN stop_id END) AS very_close_stops
    FROM {{ ref('int_restaurant_transit_access') }}
    WHERE proximity_rank <= 10  -- Top 10 nearest stops
    GROUP BY restaurant_id
),

-- Calculate component scores
scoring AS (
    SELECT
        r.restaurant_id,
        r.restaurant_name,
        r.rating,
        r.review_count,
        r.price_level_numeric,
        
        -- Safety Score (0-100)
        CASE
            WHEN i.total_inspections = 0 THEN 50  -- No data default
            WHEN i.days_since_last_inspection > 365 THEN 
                GREATEST(0, 40 - (i.days_since_last_inspection - 365) / 10)  -- Penalize old inspections
            ELSE
                GREATEST(0, LEAST(100,
                    60 +  -- Base score
                    (i.passed_inspections * 10) -
                    (i.failed_inspections * 15) -
                    (i.total_violation_points * 0.5) +
                    (CASE WHEN i.recent_passes > 0 THEN 20 ELSE 0 END) -
                    (CASE WHEN i.recent_fails > 0 THEN 25 ELSE 0 END)
                ))
        END AS safety_score,
        
        -- Accessibility Score (0-100)
        CASE
            WHEN t.nearby_stops_count = 0 THEN 0
            ELSE GREATEST(0, LEAST(100,
                -- Base score from nearest stop
                CASE
                    WHEN t.nearest_stop_distance <= 200 THEN 100
                    WHEN t.nearest_stop_distance <= 500 THEN 85
                    WHEN t.nearest_stop_distance <= 1000 THEN 60
                    ELSE 30
                END * 0.5 +
                -- Bonus for multiple stops
                (t.very_close_stops * 5) * 0.3 +
                -- Bonus for accessibility
                (t.accessible_stops_count * 3) * 0.2
            ))
        END AS accessibility_score,
        
        -- Popularity Score (0-100)
        CASE
            WHEN r.review_count = 0 OR r.rating IS NULL THEN 0
            ELSE GREATEST(0, LEAST(100,
                -- Rating component (60% weight)
                (r.rating / 5.0 * 100) * 0.6 +
                -- Review volume component (40% weight)
                (CASE
                    WHEN r.review_count >= 500 THEN 100
                    WHEN r.review_count >= 200 THEN 85
                    WHEN r.review_count >= 100 THEN 70
                    WHEN r.review_count >= 50 THEN 55
                    ELSE r.review_count * 1.1
                END) * 0.4
            ))
        END AS popularity_score,
        
        -- Value Score (0-100)
        CASE
            WHEN r.price_level_numeric IS NULL THEN 50
            WHEN r.rating IS NULL THEN 50
            ELSE GREATEST(0, LEAST(100,
                -- Price component
                ((5 - r.price_level_numeric) * 20) * 0.5 +
                -- Quality-to-price ratio
                (r.rating * 20 / NULLIF(r.price_level_numeric, 0)) * 0.5
            ))
        END AS value_score,
        
        -- Metadata
        i.total_inspections,
        i.latest_inspection_date,
        i.days_since_last_inspection,
        t.nearest_stop_distance,
        t.nearby_stops_count
        
    FROM restaurants r
    LEFT JOIN inspection_metrics i ON r.restaurant_id = i.restaurant_id
    LEFT JOIN transit_metrics t ON r.restaurant_id = t.restaurant_id
)

SELECT
    *,
    -- Calculate overall recommendation score (weighted average)
    ROUND(
        safety_score * 0.35 +
        accessibility_score * 0.15 +
        popularity_score * 0.35 +
        value_score * 0.15,
        2
    ) AS overall_score,
    
    -- Recommendation tier
    CASE
        WHEN safety_score * 0.35 + accessibility_score * 0.15 + 
             popularity_score * 0.35 + value_score * 0.15 >= 80 THEN 'HIGHLY_RECOMMENDED'
        WHEN safety_score * 0.35 + accessibility_score * 0.15 + 
             popularity_score * 0.35 + value_score * 0.15 >= 60 THEN 'RECOMMENDED'
        WHEN safety_score * 0.35 + accessibility_score * 0.15 + 
             popularity_score * 0.35 + value_score * 0.15 >= 40 THEN 'AVERAGE'
        ELSE 'BELOW_AVERAGE'
    END AS recommendation_tier,
    
    CURRENT_TIMESTAMP() AS score_calculated_at
    
FROM scoring