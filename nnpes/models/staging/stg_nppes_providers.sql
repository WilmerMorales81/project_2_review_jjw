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
            WHEN entity_type = '1' THEN 'Individual' -- entity_type of '1' indicates an individual provider, so we label it as 'Individual'.
            WHEN entity_type = '2' THEN 'Organization' -- entity_type of '2' indicates an organization provider, so we label it as 'Organization'.
            ELSE 'Unknown' -- any other entity_type is labeled as 'Unknown'.
        END AS provider_type,

        CASE
            WHEN entity_type = '1' THEN first_name || ' ' || last_name -- for individual providers, concatenate first and last names
            ELSE organization_name -- for organization providers, use the organization name
        END AS provider_name,

        first_name,
        last_name,
        organization_name,
        state,
        zip_code,
        taxonomy_code,
        CURRENT_TIMESTAMP AS loaded_at  -- timestamp when the data was loaded

    FROM source
    WHERE NPI IS NOT NULL
      AND state IS NOT NULL
      AND zip_code IS NOT NULL
)

SELECT * FROM cleaned
