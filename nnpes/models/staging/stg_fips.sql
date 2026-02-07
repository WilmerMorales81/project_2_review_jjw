-- models/staging/stg_fips.sql
-- State and county FIPS codes with names

{{
  config(
    materialized='view',
    tags=['staging', 'reference']
  )
}}

WITH source AS (
    SELECT *
    FROM read_csv('../data/raw/ssa_fips_state_county_2025.csv')
),

cleaned AS (
    SELECT
        state::VARCHAR AS state_code,
        state_name::VARCHAR AS state_name,
        fipscounty::VARCHAR AS county_fips,
        countyname_fips::VARCHAR AS county_name,
        CURRENT_TIMESTAMP AS loaded_at
    FROM source
    WHERE fipscounty IS NOT NULL
      AND state IS NOT NULL
)

SELECT * FROM cleaned
