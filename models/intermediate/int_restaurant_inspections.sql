{{
    config(
        materialized='incremental',
        unique_key='match_id',
        on_schema_change='merge_columns'
    )
}}

WITH restaurants AS (
    SELECT * FROM {{ ref('stg_yelp_restaurants') }}
),

inspections AS (
    SELECT * FROM {{ ref('stg_health_inspections') }}
    {% if is_incremental() %}
    WHERE staging_processed_at >= (SELECT MAX(match_processed_at) FROM {{ this }})
    {% endif %}
),

-- Fuzzy matching logic
name_matching AS (
    SELECT 
        r.restaurant_id,
        r.restaurant_name,
        r.street_address AS restaurant_address,
        r.city AS restaurant_city,
        i.inspection_id,
        i.license_no,
        i.business_name_dba,
        i.street_address AS inspection_address,
        i.city AS inspection_city,
        i.inspection_date,
        i.inspection_result,
        i.violation_code,
        i.violation_severity,
        i.violation_severity_score,
        i.violation_description,
        
        -- Calculate match confidence scores
        CASE
            -- Exact name match
            WHEN UPPER(r.restaurant_name) = i.business_name_dba THEN 100
            WHEN UPPER(r.restaurant_name) = i.business_name_legal THEN 95
            
            -- Partial name match
            WHEN CONTAINS(UPPER(r.restaurant_name), i.business_name_dba) 
                OR CONTAINS(i.business_name_dba, UPPER(r.restaurant_name)) THEN 85
            
            -- Fuzzy name match (Levenshtein distance)
            WHEN EDITDISTANCE(UPPER(r.restaurant_name), i.business_name_dba) <= 3 THEN 75
            WHEN EDITDISTANCE(UPPER(r.restaurant_name), i.business_name_dba) <= 5 THEN 60
            
            -- Address-based match
            WHEN UPPER(r.street_address) = UPPER(i.street_address) 
                AND r.city = i.city THEN 70
            
            ELSE 0
        END AS match_confidence_score,
        
        -- Additional matching criteria
        CASE
            WHEN r.city = i.city THEN 10
            ELSE 0
        END AS location_match_bonus
        
    FROM restaurants r
    CROSS JOIN inspections i
    WHERE 
        -- Must be in same city
        UPPER(r.city) = UPPER(i.city)
        -- And have some name similarity
        AND (
            CONTAINS(UPPER(r.restaurant_name), SPLIT_PART(i.business_name_dba, ' ', 1))
            OR EDITDISTANCE(UPPER(r.restaurant_name), i.business_name_dba) <= 8
            OR UPPER(r.street_address) = UPPER(i.street_address)
        )
),

scored_matches AS (
    SELECT
        *,
        match_confidence_score + location_match_bonus AS total_match_score,
        ROW_NUMBER() OVER (
            PARTITION BY restaurant_id, inspection_id 
            ORDER BY match_confidence_score + location_match_bonus DESC
        ) AS match_rank
    FROM name_matching
    WHERE match_confidence_score > 50  -- Minimum threshold
)

SELECT
    MD5(CONCAT(restaurant_id, '|', inspection_id)) AS match_id,
    restaurant_id,
    restaurant_name,
    inspection_id,
    license_no,
    business_name_dba,
    inspection_date,
    inspection_result,
    violation_code,
    violation_severity,
    violation_severity_score,
    violation_description,
    total_match_score AS match_confidence,
    DATEDIFF(day, inspection_date, CURRENT_DATE()) AS days_since_inspection,
    CURRENT_TIMESTAMP() AS match_processed_at
FROM scored_matches
WHERE match_rank = 1