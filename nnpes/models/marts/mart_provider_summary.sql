-- models/marts/mart_provider_summary.sql
-- Overall summary statistics for the entire dataset

{{
  config(
    materialized='table',
    tags=['marts', 'summary']
  )
}}

WITH providers AS (
    SELECT *
    FROM {{ ref('stg_nppes_providers') }}
)

SELECT
    COUNT(*) AS total_providers,
    COUNT(CASE WHEN provider_type = 'Individual' THEN 1 END) AS total_individuals,
    COUNT(CASE WHEN provider_type = 'Organization' THEN 1 END) AS total_organizations,
    COUNT(DISTINCT state) AS total_states,
    COUNT(DISTINCT zip_code) AS total_zip_codes,
    COUNT(DISTINCT taxonomy_code) AS total_specialties,
    ROUND(100.0 * COUNT(CASE WHEN provider_type = 'Individual' THEN 1 END) / COUNT(*), 2) AS pct_individuals,
    ROUND(100.0 * COUNT(CASE WHEN provider_type = 'Organization' THEN 1 END) / COUNT(*), 2) AS pct_organizations,
    CURRENT_TIMESTAMP AS summary_date

FROM providers