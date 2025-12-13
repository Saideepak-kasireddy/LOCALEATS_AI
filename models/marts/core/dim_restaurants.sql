{{
    config(
        materialized='table',
        post_hook="GRANT SELECT ON {{ this }} TO ROLE LOCALEATS_ANALYST"
    )
}}

WITH restaurant_base AS (
    SELECT * FROM {{ ref('stg_yelp_restaurants') }}
),

scores AS (
    SELECT * FROM {{ ref('int_restaurant_scores') }}
),

inspection_agg AS (
    SELECT * FROM {{ ref('int_inspection_aggregates') }}
),

final AS (
    SELECT
        -- Surrogate Key
        MD5(r.restaurant_id) AS restaurant_key,
        
        -- Natural Key
        r.restaurant_id,
        
        -- Core Attributes
        r.restaurant_name,
        r.phone_number,
        r.yelp_url,
        
        -- Location Attributes
        r.street_address,
        r.street_address_2,
        r.city,
        r.state_code,
        r.postal_code,
        r.neighborhood,
        r.full_address_formatted,
        r.lat,
        r.lng,
        ST_MAKEPOINT(r.lng, r.lat) AS geo_point,
        
        -- Category Attributes
        r.primary_cuisine_type,
        r.category_aliases_list,
        r.category_titles_list,
        
        -- Business Attributes
        r.rating AS yelp_rating,
        r.review_count AS yelp_review_count,
        r.price_symbol,
        r.price_level_numeric,
        r.is_permanently_closed,
        
        -- Calculated Scores
        COALESCE(s.safety_score, 50) AS safety_score,
        COALESCE(s.accessibility_score, 50) AS accessibility_score,
        COALESCE(s.popularity_score, 50) AS popularity_score,
        COALESCE(s.value_score, 50) AS value_score,
        COALESCE(s.overall_score, 50) AS overall_recommendation_score,
        s.recommendation_tier,
        
        -- Inspection Summary
        COALESCE(i.total_inspections_all_time, 0) AS total_inspections,
        i.pass_rate AS inspection_pass_rate,
        i.performance_category AS inspection_performance,
        i.risk_level AS health_risk_level,
        s.latest_inspection_date,
        s.days_since_last_inspection,
        
        -- Transit Access
        s.nearest_stop_distance AS nearest_transit_meters,
        s.nearby_stops_count AS transit_stops_within_1km,
        
        -- Data Quality
        CASE
            WHEN s.overall_score IS NOT NULL THEN 'COMPLETE'
            WHEN r.rating IS NOT NULL THEN 'PARTIAL'
            ELSE 'MINIMAL'
        END AS data_completeness,
        
        -- Metadata
        r.bronze_loaded_at AS source_loaded_at,
        r.staging_processed_at,
        CURRENT_TIMESTAMP() AS dim_created_at,
        '{{ var("dbt_version", "1.0.0") }}' AS etl_version
        
    FROM restaurant_base r
    LEFT JOIN scores s ON r.restaurant_id = s.restaurant_id
    LEFT JOIN inspection_agg i ON r.restaurant_id = i.restaurant_id
)

SELECT * FROM final