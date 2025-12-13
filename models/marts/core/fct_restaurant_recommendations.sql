{{
    config(
        materialized='incremental',
        unique_key=['restaurant_id', 'recommendation_date'],
        on_schema_change='merge_columns',
        partition_by={'field': 'recommendation_date', 'data_type': 'date'},
        cluster_by=['neighborhood', 'primary_cuisine_type']
    )
}}

WITH restaurants AS (
    SELECT * FROM {{ ref('dim_restaurants') }}
    WHERE is_permanently_closed = FALSE
        AND overall_recommendation_score IS NOT NULL
    {% if is_incremental() %}
        AND dim_created_at >= (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
),

recommendations AS (
    SELECT
        -- Keys
        restaurant_id,
        restaurant_key,
        CURRENT_DATE() AS recommendation_date,
        
        -- Restaurant Info
        restaurant_name,
        neighborhood,
        primary_cuisine_type,
        price_symbol,
        
        -- Scores
        overall_recommendation_score,
        safety_score,
        accessibility_score,
        popularity_score,
        value_score,
        recommendation_tier,
        
        -- Rankings
        ROW_NUMBER() OVER (ORDER BY overall_recommendation_score DESC) AS city_rank,
        ROW_NUMBER() OVER (PARTITION BY neighborhood ORDER BY overall_recommendation_score DESC) AS neighborhood_rank,
        ROW_NUMBER() OVER (PARTITION BY primary_cuisine_type ORDER BY overall_recommendation_score DESC) AS cuisine_rank,
        ROW_NUMBER() OVER (PARTITION BY price_symbol ORDER BY overall_recommendation_score DESC) AS price_tier_rank,
        
        -- Percentiles
        PERCENT_RANK() OVER (ORDER BY overall_recommendation_score) AS overall_percentile,
        PERCENT_RANK() OVER (ORDER BY safety_score) AS safety_percentile,
        PERCENT_RANK() OVER (ORDER BY popularity_score) AS popularity_percentile,
        
        -- Special Categories
        CASE
            WHEN overall_recommendation_score >= 85 AND price_level_numeric <= 2 THEN TRUE
            ELSE FALSE
        END AS is_best_value,
        
        CASE
            WHEN safety_score >= 90 AND days_since_last_inspection <= 90 THEN TRUE
            ELSE FALSE
        END AS is_recently_inspected_excellent,
        
        CASE
            WHEN neighborhood_rank <= 5 THEN TRUE
            ELSE FALSE
        END AS is_top5_in_neighborhood,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS created_at
        
    FROM restaurants
)

SELECT * FROM recommendations