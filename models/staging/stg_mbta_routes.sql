WITH source_data AS (
    SELECT * FROM {{ source('bronze', 'bronze_mbta_routes') }}
),

cleaned AS (
    SELECT
        -- Identifiers
        UPPER(TRIM(route_id)) AS route_id,
        route_short_name AS route_code,
        route_long_name AS route_name,
        
        -- Route Type
        route_type AS route_type_id,
        CASE route_type
            WHEN '0' THEN 'Light Rail'
            WHEN '1' THEN 'Heavy Rail'
            WHEN '2' THEN 'Commuter Rail'
            WHEN '3' THEN 'Bus'
            WHEN '4' THEN 'Ferry'
            ELSE 'Other'
        END AS route_type_description,
        
        -- Visual Information
        CONCAT('#', UPPER(route_color)) AS route_color_hex,
        CONCAT('#', UPPER(route_text_color)) AS route_text_color_hex,
        
        -- Ordering
        route_sort_order AS display_order,
        
        -- Derived Fields
        CASE
            WHEN route_type IN ('0', '1') THEN 'RAIL'
            WHEN route_type = '3' THEN 'BUS'
            WHEN route_type = '4' THEN 'FERRY'
            ELSE 'OTHER'
        END AS transit_mode,
        
        -- Metadata
        loaded_at AS bronze_loaded_at,
        CURRENT_TIMESTAMP() AS staging_processed_at
        
    FROM source_data
    WHERE route_id IS NOT NULL
)

SELECT * FROM cleaned