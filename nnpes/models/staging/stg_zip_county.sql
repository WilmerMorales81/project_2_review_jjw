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
    FROM read_parquet('../data/cleaned/zip_county.parquet')
),

cleaned AS (
    SELECT
        zip_code,
        county_fips,
        CURRENT_TIMESTAMP AS loaded_at
    FROM source
    WHERE zip_code IS NOT NULL
      AND county_fips IS NOT NULL
)

SELECT * FROM cleaned