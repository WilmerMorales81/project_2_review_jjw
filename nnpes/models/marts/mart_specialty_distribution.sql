-- models/marts/mart_specialty_distribution.sql
-- Distribution of healthcare specialties by state

{{
  config(
    materialized='table',
    tags=['marts', 'analytics', 'specialty']
  )
}}

WITH providers AS (
    SELECT *
    FROM {{ ref('stg_nppes_providers') }}
),

taxonomy AS (
    SELECT *
    FROM {{ ref('stg_taxonomy') }}
)

SELECT
    p.state,
    t.classification AS specialty_classification,
    t.display_name AS specialty_name,
    COUNT(*) AS provider_count,
    COUNT(DISTINCT p.zip_code) AS zip_codes_covered

FROM providers p
LEFT JOIN taxonomy t
    ON p.taxonomy_code = t.taxonomy_code
WHERE t.classification IS NOT NULL
GROUP BY p.state, t.classification, t.display_name
ORDER BY p.state, provider_count DESC
