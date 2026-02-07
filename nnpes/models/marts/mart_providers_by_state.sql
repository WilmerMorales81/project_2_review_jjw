-- models/marts/mart_providers_by_state.sql
-- Providers aggregated by state

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
state_summary AS (
    SELECT
        state,
        COUNT(*) AS total_providers,
        COUNT(CASE WHEN provider_type = 'Individual' THEN 1 END) AS individual_providers,
        COUNT(CASE WHEN provider_type = 'Organization' THEN 1 END) AS organization_providers,
        COUNT(DISTINCT zip_code) AS unique_zip_codes,
        COUNT(DISTINCT taxonomy_code) AS unique_specialties

    FROM providers
    GROUP BY state
)

SELECT
    state,
    total_providers,
    individual_providers,
    organization_providers,
    unique_zip_codes,
    unique_specialties,
    ROUND(100.0 * individual_providers / total_providers, 2) AS pct_individual

FROM state_summary
ORDER BY total_providers DESC
