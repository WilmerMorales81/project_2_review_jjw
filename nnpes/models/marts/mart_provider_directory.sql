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
    p.provider_id,
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
    ON p.taxonomy_code = t.taxonomy_code
LEFT JOIN zip_county z
    ON p.zip_code = z.zip_code
LEFT JOIN county_info c
    ON z.county_fips = c.county_fips

ORDER BY p.state, p.provider_name
