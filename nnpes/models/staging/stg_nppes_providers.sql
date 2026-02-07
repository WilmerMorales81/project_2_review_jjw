-- models/staging/stg_nppes_providers.sql
-- Staging layer: Clean provider data (1:1 from source, no joins)

{{
  config(
    materialized='view',
    tags=['staging', 'nppes']
  )
}}

WITH source AS (
    SELECT *
    FROM read_parquet('../data/cleaned/nppes_cleaned.parquet')
),

-- Clean and standardize
cleaned AS (
    SELECT
        NPI::VARCHAR AS provider_id,

        CASE
            WHEN entity_type = '1' THEN 'Individual'
            WHEN entity_type = '2' THEN 'Organization'
            ELSE 'Unknown'
        END AS provider_type,

        CASE
            WHEN entity_type = '1' THEN first_name || ' ' || last_name
            ELSE organization_name
        END AS provider_name,

        first_name,
        last_name,
        organization_name,
        state,
        zip_code,
        taxonomy_code,
        CURRENT_TIMESTAMP AS loaded_at

    FROM source
    WHERE NPI IS NOT NULL
      AND state IS NOT NULL
      AND zip_code IS NOT NULL
)

SELECT * FROM cleaned
