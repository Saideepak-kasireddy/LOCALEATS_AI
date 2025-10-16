WITH source_data AS (
    SELECT * FROM {{ source('bronze', 'bronze_yelp_restaurants') }}
),

cleaned AS (
    SELECT
        -- Identifiers
        restaurant_id,
        
        -- Core Information
        TRIM(name) AS restaurant_name,
        NULLIF(TRIM(phone), '') AS phone_number,
        NULLIF(TRIM(url), '') AS yelp_url,
        
        -- Location Details
        TRIM(address) AS street_address,
        NULLIF(TRIM(address2), '') AS street_address_2,
        INITCAP(TRIM(city)) AS city,
        UPPER(TRIM(state)) AS state_code,
        REGEXP_REPLACE(TRIM(zip_code), '[^0-9-]', '') AS postal_code,
        INITCAP(NULLIF(TRIM(neighborhood), '')) AS neighborhood,
        
        -- Coordinates
        latitude AS lat,
        longitude AS lng,
        
        -- Full Address Construction
        CONCAT_WS(', ', 
            TRIM(address), 
            NULLIF(TRIM(address2), ''),
            TRIM(city), 
            CONCAT(UPPER(TRIM(state)), ' ', TRIM(zip_code))
        ) AS full_address_formatted,
        
        -- Categories and Cuisine
        COALESCE(
            primary_cuisine, 
            SPLIT_PART(category_aliases, ',', 1)
        ) AS primary_cuisine_type,
        LOWER(category_aliases) AS category_aliases_list,
        category_titles AS category_titles_list,
        
        -- Ratings and Reviews
        CAST(yelp_rating AS DECIMAL(2,1)) AS rating,
        yelp_review_count AS review_count,
        
        -- Pricing
        price_tier AS price_symbol,
        CASE 
            WHEN price_tier = '$' THEN 1
            WHEN price_tier = '$$' THEN 2
            WHEN price_tier = '$$$' THEN 3
            WHEN price_tier = '$$$$' THEN 4
            ELSE NULL
        END AS price_level_numeric,
        
        -- Status
        COALESCE(is_closed, FALSE) AS is_permanently_closed,
        
        -- Metadata
        api_key_used AS source_api_key,
        search_location AS search_neighborhood,
        loaded_at AS bronze_loaded_at,
        CURRENT_TIMESTAMP() AS staging_processed_at
        
    FROM source_data
    WHERE restaurant_id IS NOT NULL
        AND name IS NOT NULL
        AND latitude IS NOT NULL
        AND longitude IS NOT NULL
        AND latitude BETWEEN 40 AND 45  -- Boston area bounds
        AND longitude BETWEEN -72 AND -70
)

SELECT * FROM cleaned