-- models/staging/stg_census.sql
-- County demographic and geographic info

{{
  config(
    materialized='view',
    tags=['staging', 'reference']
  )
}}

WITH source AS (    -- source step to read the raw CSV file containing census data, which includes county FIPS codes, county names, state abbreviations, and state names. This step is crucial for bringing the raw data into the dbt environment for further cleaning and transformation. Make sure the file path is correct and that the CSV file is properly formatted for successful reading.
    SELECT *
    FROM read_csv('../data/raw/ssa_fips_state_county_2025.csv')
),

cleaned AS (
    SELECT
        fipscounty::VARCHAR AS county_fips,   -- varchar is a good choice for FIPS codes because they are often treated as strings (to preserve leading zeros) rather than numeric values. This ensures that the FIPS codes are stored and processed correctly without losing any important formatting.
        countyname_fips::VARCHAR AS county_name,   -- varchar is also suitable for county names to handle any special characters or varying lengths.
        state::VARCHAR AS state_abbr,  -- varchar is appropriate for state abbreviations to preserve the two-letter format.
        state_name::VARCHAR AS state_name,   -- varchar is suitable for state names to handle any special characters or varying lengths.
        CURRENT_TIMESTAMP AS loaded_at
    FROM source
    WHERE fipscounty IS NOT NULL
      AND state IS NOT NULL
)

SELECT * FROM cleaned