-- DuckDB Query Examples
-- Use Ctrl+Enter to run queries in DataGrip or your SQL client

-- 1. Show all available tables
SHOW TABLES;

-- 2. View provider summary
SELECT * FROM mart_provider_summary;

-- 3. Top 10 states by provider count
SELECT
    state,
    total_providers
FROM mart_providers_by_state
ORDER BY total_providers DESC
LIMIT 10;

-- 4. View providers in a specific state (example: California)
SELECT
    provider_name,
    provider_type,
    state,
    zip_code,
    specialty_name
FROM mart_provider_directory
WHERE state = 'CA'
LIMIT 20;

-- 5. Count providers by specialty classification
SELECT
    specialty_classification,
    COUNT(*) as provider_count
FROM mart_provider_directory
WHERE specialty_classification IS NOT NULL
GROUP BY specialty_classification
ORDER BY provider_count DESC
LIMIT 15;

-- 6. View table structure (select a table)
DESCRIBE mart_provider_summary;

-- 7. Quick data overview with statistics
SUMMARIZE mart_providers_by_state;
