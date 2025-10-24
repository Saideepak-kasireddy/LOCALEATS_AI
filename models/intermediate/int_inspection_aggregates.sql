-- models/intermediate/int_inspection_aggregates.sql
{{
    config(
        materialized='table'
    )
}}

WITH inspection_base AS (
    SELECT * FROM {{ ref('int_restaurant_inspections') }}
),

inspection_trends AS (
    SELECT
        restaurant_id,
        restaurant_name,
        COUNT(DISTINCT inspection_id) AS total_inspections_all_time,
        
        -- Overall performance
        AVG(CASE WHEN inspection_result = 'PASS' THEN 1.0 ELSE 0.0 END) * 100 AS pass_rate,
        COUNT(DISTINCT violation_code) AS unique_violation_types,
        
        -- Recent 6 months performance
        COUNT(DISTINCT CASE 
            WHEN days_since_inspection <= 180 
            THEN inspection_id 
        END) AS recent_inspection_count,
        
        AVG(CASE 
            WHEN days_since_inspection <= 180 AND inspection_result = 'PASS' THEN 1.0
            WHEN days_since_inspection <= 180 AND inspection_result = 'FAIL' THEN 0.0
            ELSE NULL
        END) * 100 AS recent_pass_rate,
        
        -- Critical violations
        COUNT(DISTINCT CASE 
            WHEN violation_severity = 'HIGH' 
            THEN violation_code 
        END) AS critical_violation_count,
        
        SUM(violation_severity_score) AS total_violation_score,
        
        -- Most recent inspection
        MIN(days_since_inspection) AS days_since_last_inspection,
        MAX(inspection_date) AS latest_inspection_date
        
    FROM inspection_base
    GROUP BY 1, 2
)

SELECT
    *,
    -- Performance category
    CASE
        WHEN COALESCE(recent_pass_rate, pass_rate) >= 95 THEN 'EXCELLENT'
        WHEN COALESCE(recent_pass_rate, pass_rate) >= 85 THEN 'GOOD'
        WHEN COALESCE(recent_pass_rate, pass_rate) >= 70 THEN 'SATISFACTORY'
        WHEN COALESCE(recent_pass_rate, pass_rate) >= 50 THEN 'NEEDS_IMPROVEMENT'
        ELSE 'POOR'
    END AS performance_category,
    
    -- Risk level
    CASE
        WHEN critical_violation_count = 0 AND COALESCE(recent_pass_rate, 100) >= 90 THEN 'LOW_RISK'
        WHEN critical_violation_count <= 2 AND COALESCE(recent_pass_rate, 100) >= 70 THEN 'MEDIUM_RISK'
        ELSE 'HIGH_RISK'
    END AS health_risk_level,
    
    CURRENT_TIMESTAMP() AS aggregation_timestamp
    
FROM inspection_trends