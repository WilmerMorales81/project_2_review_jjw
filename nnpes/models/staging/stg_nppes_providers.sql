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
    FROM read_csv('../data/raw/npidata_pfile.csv')
),

-- Select and rename columns
selected AS (
    SELECT
        "NPI",
        "Entity Type Code" AS entity_type,
        "Provider Organization Name (Legal Business Name)" AS organization_name,
        "Provider Last Name (Legal Name)" AS last_name,
        "Provider First Name" AS first_name,
        "Provider Business Practice Location Address State Name" AS state,
        "Provider Business Practice Location Address Postal Code" AS zip_code_raw,
        "Healthcare Provider Taxonomy Code_1" AS taxonomy_code
    FROM source
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
        SUBSTRING(zip_code_raw::VARCHAR, 1, 5) AS zip_code,  -- Keep only first 5 digits of ZIP code
        taxonomy_code,
        CURRENT_TIMESTAMP AS loaded_at  -- timestamp when the data was loaded

    FROM selected
    WHERE NPI IS NOT NULL
      AND state IS NOT NULL
      AND zip_code_raw IS NOT NULL
)

SELECT * FROM cleaned
