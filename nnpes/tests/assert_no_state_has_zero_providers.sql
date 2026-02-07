-- Test: Check no state has zero providers

SELECT
    state,
    total_providers
FROM {{ ref('mart_providers_by_state') }}
WHERE total_providers = 0
  OR total_providers IS NULL
