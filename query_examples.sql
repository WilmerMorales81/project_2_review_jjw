-- DuckDB 查询示例
-- 使用 Ctrl+Enter 运行查询

-- 1. 查看所有表
SHOW TABLES;

-- 2. 查看提供商总结
SELECT * FROM mart_provider_summary;

-- 3. 查看前 10 个州（按提供商数量排序）
SELECT
    state,
    total_providers
FROM mart_providers_by_state
ORDER BY total_providers DESC
LIMIT 10;

-- 4. 查看特定州的提供商（例如：California）
SELECT
    provider_name,
    provider_type,
    state,
    zip_code,
    specialty_name
FROM mart_provider_directory
WHERE state = 'CA'
LIMIT 20;

-- 5. 按专业分类统计提供商数量
SELECT
    specialty_classification,
    COUNT(*) as provider_count
FROM mart_provider_directory
WHERE specialty_classification IS NOT NULL
GROUP BY specialty_classification
ORDER BY provider_count DESC
LIMIT 15;

-- 6. 查看表结构（选择一个表）
DESCRIBE mart_provider_summary;

-- 7. 数据概览（快速统计）
SUMMARIZE mart_providers_by_state;
