-- models/staging/stg_zip_county.sql
-- ZIP to county mapping

{{
  config(
    materialized='view',
    tags=['staging', 'geographic']
  )
}}
WITH source AS (
    SELECT *
    FROM read_csv('../data/raw/ZIP_COUNTY_032025.csv')
),

-- Select and format columns
formatted AS (
    SELECT
        LPAD(ZIP::VARCHAR, 5, '0') AS zip_code,  -- Ensure ZIP codes are 5-digit strings with leading zeros
        LPAD(COUNTY::VARCHAR, 5, '0') AS county_fips,  -- Ensure county FIPS codes are 5-digit strings
        RES_RATIO AS res_ratio  -- Residential ratio to determine primary county for ZIP
    FROM source
),

-- Keep primary county for each ZIP (highest residential ratio)
cleaned AS (
    SELECT
        zip_code,
        county_fips,
        CURRENT_TIMESTAMP AS loaded_at
    FROM (
        SELECT
            zip_code,
            county_fips,
            ROW_NUMBER() OVER (PARTITION BY zip_code ORDER BY res_ratio DESC) AS rn
        FROM formatted
        WHERE zip_code IS NOT NULL
          AND county_fips IS NOT NULL
    ) ranked
    WHERE rn = 1
)

SELECT * FROM cleaned