{{
    config(
        materialized='view'
    )
}}

WITH base_data AS (
    SELECT
        d.restaurant_id,
        d.restaurant_name,
        d.neighborhood,
        d.primary_cuisine_type,
        d.safety_score,
        d.health_risk_level,
        d.inspection_pass_rate,
        d.total_inspections,
        d.days_since_last_inspection,
        d.latest_inspection_date,
        i.recent_inspection_count,
        i.recent_pass_rate,
        i.critical_violation_count,
        i.performance_category
    FROM {{ ref('dim_restaurants') }} d
    LEFT JOIN {{ ref('int_inspection_aggregates') }} i
        ON d.restaurant_id = i.restaurant_id
),

safety_summary AS (
    SELECT
        restaurant_id,
        restaurant_name,
        neighborhood,
        primary_cuisine_type,
        safety_score,
        health_risk_level,
        inspection_pass_rate,
        total_inspections,
        days_since_last_inspection,
        
        -- Safety Status
        CASE
            WHEN days_since_last_inspection IS NULL THEN 'NO_INSPECTION_DATA'
            WHEN days_since_last_inspection <= 90 THEN 'RECENTLY_INSPECTED'
            WHEN days_since_last_inspection <= 180 THEN 'STANDARD_SCHEDULE'
            WHEN days_since_last_inspection <= 365 THEN 'INSPECTION_DUE'
            ELSE 'OVERDUE_FOR_INSPECTION'
        END AS inspection_status,
        
        -- Alert Flags
        CASE
            WHEN health_risk_level = 'HIGH_RISK' THEN TRUE
            WHEN critical_violation_count > 3 THEN TRUE
            WHEN recent_pass_rate < 50 THEN TRUE
            ELSE FALSE
        END AS requires_attention,
        
        -- Improvement Indicator
        CASE
            WHEN recent_pass_rate > inspection_pass_rate + 10 THEN 'IMPROVING'
            WHEN recent_pass_rate < inspection_pass_rate - 10 THEN 'DECLINING'
            ELSE 'STABLE'
        END AS performance_trend
        
    FROM base_data
)

SELECT * FROM safety_summary
ORDER BY requires_attention DESC, safety_score ASC