-- models/staging/stg_health_inspections.sql
WITH source_data AS (
    SELECT * FROM {{ source('bronze', 'bronze_health_inspections') }}
),

-- Filter inactive licenses and deduplicate
active_deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY licenseno, resultdttm, violation 
            ORDER BY violdttm DESC NULLS LAST
        ) AS rn
    FROM source_data
    WHERE UPPER(COALESCE(licstatus, 'INACTIVE')) = 'ACTIVE'  -- Fixed filter
),

cleaned AS (
    SELECT
        -- Generate unique inspection ID
        MD5(CONCAT(
            COALESCE(licenseno, 'NA'), '|',
            COALESCE(CAST(resultdttm AS STRING), 'NA'), '|',
            COALESCE(violation, 'NA')
        )) AS inspection_id,
        
        -- Business Information (NO dbaname, legalowner, namelast, namefirst)
        REGEXP_REPLACE(
            TRIM(UPPER(COALESCE(businessname, 'UNKNOWN_BUSINESS'))), 
            '\\s+', ' '
        ) AS business_name,
        
        -- License Info
        UPPER(TRIM(licenseno)) AS license_no,
        COALESCE(issdttm::DATE, CURRENT_DATE()) AS license_issue_date,
        COALESCE(expdttm::DATE, DATEADD(year, 1, COALESCE(issdttm::DATE, CURRENT_DATE()))) AS license_expiry_date,
        'ACTIVE' AS license_status,  -- All are active after filter
        
        COALESCE(TRIM(licensecat), 'UNKNOWN') AS license_category_raw,
        CASE
            WHEN licensecat ILIKE '%food%' THEN 'Food Service'
            WHEN licensecat ILIKE '%retail%' THEN 'Retail Food'
            WHEN licensecat ILIKE '%mobile%' THEN 'Mobile Food'
            WHEN licensecat IS NULL THEN 'Unknown'
            ELSE 'Other'
        END AS license_category,
        
        COALESCE(TRIM(descript), 'No description') AS license_description,
        
        -- Inspection Information
        COALESCE(resultdttm::DATE, issdttm::DATE, CURRENT_DATE()) AS inspection_date,
        CASE 
            WHEN UPPER(COALESCE(result, '')) IN ('PASS', 'PASSED', 'APPROVED', 'HE_PASS') THEN 'PASS'
            WHEN UPPER(COALESCE(result, '')) IN ('FAIL', 'FAILED', 'REJECTED', 'HE_FAIL') THEN 'FAIL'
            WHEN UPPER(COALESCE(result, '')) LIKE '%CONDITION%' THEN 'CONDITIONAL'
            WHEN result IS NULL THEN 'NO_RESULT'
            ELSE 'OTHER'
        END AS inspection_result,
        
        -- Violation Details
        COALESCE(violation, 'NO_VIOLATION') AS violation_code,
        CASE
            WHEN viol_level = '*' THEN 'LOW'
            WHEN viol_level = '**' THEN 'MEDIUM'
            WHEN viol_level = '***' THEN 'HIGH'
            WHEN viol_level IS NULL THEN 'NONE'
            ELSE 'NONE'
        END AS violation_severity,
        CASE
            WHEN viol_level = '*' THEN 1
            WHEN viol_level = '**' THEN 2
            WHEN viol_level = '***' THEN 3
            ELSE 0
        END AS violation_severity_score,
        
        COALESCE(LEFT(violdesc, 1000), 'No violation description') AS violation_description,
        violdttm::DATE AS violation_date,
        COALESCE(UPPER(TRIM(viol_status)), 'UNKNOWN') AS violation_status,
        status_date::DATE AS violation_status_update_date,
        
        -- Comments
        COALESCE(LEFT(comments, 2000), '') AS inspector_comments,
        
        -- Location
        COALESCE(TRIM(address), 'NO_ADDRESS') AS street_address,
        COALESCE(INITCAP(TRIM(city)), 'Boston') AS city,  
        COALESCE(UPPER(TRIM(state)), 'MA') AS state_code,
        
        -- ZIP code standardization
        CASE 
            WHEN zip IS NULL OR TRIM(zip) = '' THEN '00000'
            WHEN LENGTH(TRIM(zip)) = 4 THEN LPAD(TRIM(zip), 5, '0')
            WHEN LENGTH(TRIM(zip)) = 10 AND zip LIKE '%-%' THEN LEFT(zip, 5)
            WHEN REGEXP_LIKE(TRIM(zip), '^[0-9]{5}$') THEN TRIM(zip)
            ELSE '99999'
        END AS postal_code,
        
        COALESCE(property_id, 'NO_PROPERTY_ID') AS property_id,
        COALESCE(location, '') AS location_string,
        
        -- Processing Metadata
        CURRENT_TIMESTAMP() AS staging_processed_at
        
    FROM active_deduped
    WHERE rn = 1
        AND licenseno IS NOT NULL
)

SELECT * FROM cleaned