-- models/marts/core/gold_restaurant_embeddings.sql
{{
    config(
        materialized='table',
        tags=['gold', 'embeddings']
    )
}}

WITH restaurant_master AS (
    SELECT * FROM {{ ref('gold_restaurants_master') }}
)

SELECT
    restaurant_id,
    restaurant_name,
    search_description,
    
    -- Generate embeddings using Snowflake Cortex
    -- Using 'snowflake-arctic-embed-m' model (768 dimensions)
    SNOWFLAKE.CORTEX.EMBED_TEXT_768(
        'snowflake-arctic-embed-m',
        search_description
    ) as description_embedding,
    
    -- Store metadata for filtering
    city,
    neighborhood,
    primary_cuisine,
    price_level,
    overall_score,
    recommendation_tier,
    safety_score,
    health_risk_level,
    nearest_stop_distance_m,
    
    -- Google Places metadata for filtering
    serves_vegetarian,
    serves_breakfast,
    serves_lunch,
    serves_dinner,
    reservable,
    takeout,
    delivery,
    outdoor_seating,
    is_wheelchair_accessible,
    good_for_groups,
    good_for_children,
    is_currently_open,
    opening_hours_text,
    
    CURRENT_TIMESTAMP() as embedding_created_at

FROM restaurant_master
WHERE search_description IS NOT NULL