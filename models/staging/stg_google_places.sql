-- models/staging/stg_google_places.sql

WITH source AS (
    SELECT * FROM {{ source('bronze', 'bronze_google_places_enrichment') }}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY RESTAURANT_ID ORDER BY ENRICHED_AT DESC) as rn
    FROM source
),

cleaned AS (
    SELECT
        -- Primary Keys
        RESTAURANT_ID,
        GOOGLE_PLACE_ID,
        
        -- Basic Info
        GOOGLE_NAME,
        GOOGLE_ADDRESS,
        GOOGLE_RATING,
        GOOGLE_REVIEW_COUNT,
        GOOGLE_PRICE_LEVEL,
        
        -- Operational Status
        BUSINESS_STATUS,
        CASE 
            WHEN BUSINESS_STATUS = 'OPERATIONAL' THEN TRUE
            WHEN BUSINESS_STATUS IS NULL THEN TRUE
            ELSE FALSE
        END AS is_operational,
        
        -- Hours & Availability
        OPENING_HOURS_TEXT,
        OPEN_NOW,
        
        -- Dietary Options (default FALSE if NULL)
        COALESCE(SERVES_COFFEE, FALSE) AS serves_coffee,
        COALESCE(SERVES_DESSERT, FALSE) AS serves_dessert,
        COALESCE(SERVES_BREAKFAST, FALSE) AS serves_breakfast,
        COALESCE(SERVES_LUNCH, FALSE) AS serves_lunch,
        COALESCE(SERVES_DINNER, FALSE) AS serves_dinner,
        COALESCE(SERVES_BEER, FALSE) AS serves_beer,
        COALESCE(SERVES_WINE, FALSE) AS serves_wine,
        COALESCE(SERVES_VEGETARIAN, FALSE) AS serves_vegetarian,
        
        -- Service Options
        DINE_IN,
        TAKEOUT,
        DELIVERY,
        RESERVABLE,
        OUTDOOR_SEATING,
        
        -- Accessibility
        WHEELCHAIR_ACCESSIBLE_ENTRANCE,
        WHEELCHAIR_ACCESSIBLE_PARKING,
        WHEELCHAIR_ACCESSIBLE_RESTROOM,
        WHEELCHAIR_ACCESSIBLE_SEATING,
        
        CASE 
            WHEN WHEELCHAIR_ACCESSIBLE_ENTRANCE = TRUE 
             AND WHEELCHAIR_ACCESSIBLE_SEATING = TRUE 
            THEN TRUE
            WHEN WHEELCHAIR_ACCESSIBLE_ENTRANCE = TRUE 
            THEN TRUE
            ELSE FALSE
        END AS is_wheelchair_accessible,
        
        -- Group & Atmosphere
        GOOD_FOR_CHILDREN,
        COALESCE(GOOD_FOR_GROUPS, FALSE) AS good_for_groups,
        ALLOWS_DOGS,
        LIVE_MUSIC,
        HAS_RESTROOM,
        
        -- Metadata
        ENRICHED_AT
        
    FROM deduplicated
    WHERE rn = 1  -- Keep only the most recent enrichment per restaurant
)

SELECT * FROM cleaned