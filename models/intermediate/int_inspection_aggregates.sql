{{
    config(
        materialized='table'
    )
}}

WITH inspection_base AS (
    SELECT * FROM {{ ref('int_restaurant_inspections') }}
),

-- Aggregations without monthly breakdown
inspection_trends AS (
    SELECT
        restaurant_id,
        restaurant_name,
        COUNT(DISTINCT inspection_id) AS total_inspections_all_time,
        
        -- Overall metrics
        AVG(CASE WHEN inspection_result = 'PASS' THEN 1.0 ELSE 0.0 END) * 100 AS pass_rate,
        COUNT(DISTINCT violation_code) AS unique_violation_types,
        
        -- Recent performance (last 6 months)
        COUNT(DISTINCT CASE 
            WHEN days_since_inspection <= 180 
            THEN inspection_id 
        END) AS recent_inspection_count,
        
        AVG(CASE 
            WHEN days_since_inspection <= 180 AND inspection_result = 'PASS' 
            THEN 1.0
            WHEN days_since_inspection <= 180 AND inspection_result = 'FAIL'
            THEN 0.0
            ELSE NULL
        END) * 100 AS recent_pass_rate,
        
        -- Fixed LISTAGG - remove ORDER BY when using DISTINCT with CASE
        LISTAGG(DISTINCT 
            CASE 
                WHEN violation_severity = 'HIGH' 
                THEN violation_code 
            END, ', '
        ) AS critical_violations_list,
        
        COUNT(DISTINCT CASE 
            WHEN violation_severity = 'HIGH' 
            THEN violation_code 
        END) AS critical_violation_count
        
    FROM inspection_base
    GROUP BY 1, 2
)

SELECT
    t.*,
    
    -- Performance classification
    CASE
        WHEN t.recent_pass_rate >= 95 THEN 'EXCELLENT'
        WHEN t.recent_pass_rate >= 85 THEN 'GOOD'
        WHEN t.recent_pass_rate >= 70 THEN 'SATISFACTORY'
        WHEN t.recent_pass_rate >= 50 THEN 'NEEDS_IMPROVEMENT'
        WHEN t.recent_pass_rate IS NULL THEN 'NO_DATA'
        ELSE 'POOR'
    END AS performance_category,
    
    -- Risk level
    CASE
        WHEN t.critical_violation_count = 0 AND COALESCE(t.recent_pass_rate, 0) >= 90 THEN 'LOW_RISK'
        WHEN t.critical_violation_count <= 2 AND COALESCE(t.recent_pass_rate, 0) >= 70 THEN 'MEDIUM_RISK'
        WHEN t.recent_pass_rate IS NULL THEN 'UNKNOWN_RISK'
        ELSE 'HIGH_RISK'
    END AS risk_level,
    
    CURRENT_TIMESTAMP() AS aggregation_timestamp
    
FROM inspection_trends t