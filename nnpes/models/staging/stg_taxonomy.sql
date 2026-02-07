-- models/staging/stg_taxonomy.sql
-- Author: JING 
-- Purpose: [Staging] Staging model for NPPES Taxonomy data
-- Source: data/processed/nppes_cleaned.parquet
-- Status: 🚧 Framework only - TODO: implement logic

{{
  config(
    materialized='view',
    tags=['staging', 'nppes']
  )
}}

-- TODO: Implement CTE structure
-- WITH source AS (
--     SELECT * FROM ...
-- ),
--
-- cleaned AS (
--     SELECT
--         -- TODO: Add columns
--     FROM source
-- )
--
-- SELECT * FROM cleaned

-- Placeholder for now
SELECT 'TODO: Implement staging logic' as status