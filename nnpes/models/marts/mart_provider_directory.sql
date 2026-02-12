-- models/marts/mart_provider_directory.sql
-- Enriched provider directory with specialty and location info

{{
  config(
    materialized='table',
    tags=['marts', 'analytics']
  )
}}

WITH providers AS (
    SELECT *
    FROM {{ ref('stg_nppes_providers') }}
),

taxonomy AS (
    SELECT *
    FROM {{ ref('stg_taxonomy') }}
),

zip_county AS (
    SELECT *
    FROM {{ ref('stg_zip_county') }}
),

county_info AS (
    SELECT *
    FROM {{ ref('stg_census') }}
)

SELECT
    p.provider_id, -- p means provider, t means taxonomy, z means zip_county, c means county_info. This query is selecting the provider_id from the providers table (aliased as p) to include in the final enriched provider directory. The provider_id serves as a unique identifier for each healthcare provider in the dataset, allowing for easy reference and analysis when combined with other attributes such as provider type, specialty classification, and location information.
    p.provider_type,
    p.provider_name,
    p.state,
    p.zip_code,
    t.classification AS specialty_classification,
    t.display_name AS specialty_name,
    z.county_fips,
    c.county_name,
    c.state_name

FROM providers p
LEFT JOIN taxonomy t
    ON p.taxonomy_code = t.taxonomy_code -- why left join? Because not all providers may have a taxonomy code that matches the taxonomy table, and we still want to include those providers in the final directory with null values for specialty classification and name. This allows us to retain all provider records while enriching the data with specialty information where available, without excluding providers that lack taxonomy codes.
LEFT JOIN zip_county z
    ON p.zip_code = z.zip_code -- why left join? Because not all providers may have a zip code that matches the zip_county table, and we still want to include those providers in the final directory with null values for county_fips. This allows us to retain all provider records while enriching the data with county information where available, without excluding providers that lack matching zip codes.
LEFT JOIN county_info c
    ON z.county_fips = c.county_fips -- why left join? Because not all zip codes may have a matching county_fips in the county_info table, and we still want to include those providers in the final directory with null values for county_name and state_name. This allows us to retain all provider records while enriching the data with county and state information where available, without excluding providers that lack matching county_fips.

ORDER BY p.state, p.provider_name
