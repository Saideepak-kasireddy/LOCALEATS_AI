-- models/staging/stg_mbta_stops.sql
WITH source_data AS (
    SELECT * FROM {{ source('bronze', 'bronze_mbta_stops') }}
),

cleaned AS (
    SELECT
        -- Identifiers
        UPPER(TRIM(stop_id)) AS stop_id,
        TRIM(stop_name) AS stop_name,
        
        -- Location (keeping original column names)
        latitude,
        longitude,
        
        -- Municipality info
        INITCAP(TRIM(municipality)) AS municipality,
        
        -- Accessibility
        COALESCE(wheelchair_boarding, FALSE) AS is_wheelchair_accessible,
        CASE
            WHEN wheelchair_boarding = TRUE THEN 'ACCESSIBLE'
            WHEN wheelchair_boarding = FALSE THEN 'NOT_ACCESSIBLE'
            ELSE 'UNKNOWN'
        END AS accessibility_status,
        
        -- Metadata
        loaded_at AS bronze_loaded_at,
        CURRENT_TIMESTAMP() AS staging_processed_at
        
    FROM source_data
    WHERE stop_id IS NOT NULL
        AND stop_name IS NOT NULL
        -- Filter out records with missing coordinates (838 rows)
        AND latitude IS NOT NULL
        AND longitude IS NOT NULL
        -- Additional validation for Boston area bounds
        AND latitude BETWEEN 41.5 AND 43.0
        AND longitude BETWEEN -72.0 AND -70.0
)

SELECT * FROM cleaned