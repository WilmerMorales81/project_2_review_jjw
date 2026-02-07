-- models/marts/mart_providers_by_county.sql
-- Providers aggregated by county (with ZIP-county join)

{{
  config(
    materialized='table',
    tags=['marts', 'analytics', 'geographic']
  )
}}

WITH providers AS (
    SELECT *
    FROM {{ ref('stg_nppes_providers') }}
),

zip_county AS (
    SELECT *
    FROM {{ ref('stg_zip_county') }}
),

providers_with_county AS (
    SELECT
        p.provider_id,
        p.provider_type,
        p.provider_name,
        p.state,
        p.zip_code,
        p.taxonomy_code,
        z.county_fips

    FROM providers p
    LEFT JOIN zip_county z
        ON p.zip_code = z.zip_code
),

county_summary AS (
    SELECT
        state,
        county_fips,
        COUNT(*) AS total_providers,
        COUNT(CASE WHEN provider_type = 'Individual' THEN 1 END) AS individual_providers,
        COUNT(CASE WHEN provider_type = 'Organization' THEN 1 END) AS organization_providers,
        COUNT(DISTINCT zip_code) AS unique_zip_codes

    FROM providers_with_county
    WHERE county_fips IS NOT NULL
    GROUP BY state, county_fips
)

SELECT
    state,
    county_fips,
    total_providers,
    individual_providers,
    organization_providers,
    unique_zip_codes

FROM county_summary
ORDER BY state, total_providers DESC
