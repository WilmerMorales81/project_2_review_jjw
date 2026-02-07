-- models/staging/stg_taxonomy.sql
-- Healthcare provider taxonomy codes (specialties)

{{
  config(
    materialized='view',
    tags=['staging', 'reference']
  )
}}

WITH source AS (
    SELECT *
    FROM read_csv('../data/raw/nucc_taxonomy_250.csv')
),

cleaned AS (
    SELECT
        "Code"::VARCHAR AS taxonomy_code,
        "Classification"::VARCHAR AS classification,
        "Specialization"::VARCHAR AS specialization,
        "Display Name"::VARCHAR AS display_name,
        CURRENT_TIMESTAMP AS loaded_at
    FROM source
    WHERE "Code" IS NOT NULL
)

SELECT * FROM cleaned