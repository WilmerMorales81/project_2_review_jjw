-- models/marts/mart_providers_by_geography.sql
-- Provider counts by state and county with names

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

county_info AS (
    SELECT *
    FROM {{ ref('stg_census') }}
),

providers_with_location AS (
    SELECT
        p.provider_id,
        p.provider_type,
        p.state,
        p.zip_code,
        z.county_fips,
        c.county_name,
        c.state_name
    FROM providers p
    LEFT JOIN zip_county z ON p.zip_code = z.zip_code
    LEFT JOIN county_info c ON z.county_fips = c.county_fips
)

SELECT
    state,
    state_name,
    county_fips,
    county_name,
    COUNT(*) AS total_providers,
    COUNT(CASE WHEN provider_type = 'Individual' THEN 1 END) AS individual_providers,
    COUNT(CASE WHEN provider_type = 'Organization' THEN 1 END) AS organization_providers

FROM providers_with_location
WHERE county_fips IS NOT NULL
GROUP BY state, state_name, county_fips, county_name
ORDER BY state, total_providers DESC
