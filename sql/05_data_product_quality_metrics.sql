CREATE OR REPLACE TABLE data_product_quality_metrics AS
WITH base AS (
    SELECT * FROM gold_monthly_borough_stats
), totals AS (
    SELECT COUNT(*) AS total_rows FROM base
), metrics AS (
    SELECT
        'gold_row_count' AS metric_name,
        'Number of records in the Gold data product table after pipeline execution.' AS metric_definition,
        CAST(COUNT(*) AS VARCHAR) AS current_value,
        '> 0' AS expected_threshold,
        CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
        'Every pipeline run' AS update_cadence
    FROM base

    UNION ALL

    SELECT
        'gold_key_completeness',
        '% of Gold rows where trip_month, pickup_borough and dropoff_borough are not null.',
        CAST(ROUND(100.0 * SUM(CASE WHEN trip_month IS NOT NULL AND pickup_borough IS NOT NULL AND dropoff_borough IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS VARCHAR) || '%',
        '>= 95%',
        CASE WHEN 100.0 * SUM(CASE WHEN trip_month IS NOT NULL AND pickup_borough IS NOT NULL AND dropoff_borough IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) >= 95 THEN 'PASS' ELSE 'FAIL' END,
        'Every pipeline run'
    FROM base

    UNION ALL

    SELECT
        'positive_trip_count_validity',
        '% of Gold rows where trip_count is greater than 0.',
        CAST(ROUND(100.0 * SUM(CASE WHEN trip_count > 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS VARCHAR) || '%',
        '100%',
        CASE WHEN SUM(CASE WHEN trip_count > 0 THEN 1 ELSE 0 END) = COUNT(*) AND COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
        'Every pipeline run'
    FROM base

    UNION ALL

    SELECT
        'non_negative_amount_validity',
        '% of Gold rows where avg_total_amount and total_revenue are non-negative.',
        CAST(ROUND(100.0 * SUM(CASE WHEN avg_total_amount >= 0 AND total_revenue >= 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS VARCHAR) || '%',
        '>= 99%',
        CASE WHEN 100.0 * SUM(CASE WHEN avg_total_amount >= 0 AND total_revenue >= 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) >= 99 THEN 'PASS' ELSE 'FAIL' END,
        'Every pipeline run'
    FROM base

    UNION ALL

    SELECT
        'data_freshness_months',
        'Difference in months between current date and the latest trip_month available in the Gold table.',
        CAST(DATE_DIFF('month', MAX(trip_month), CURRENT_DATE) AS VARCHAR),
        '<= 6 months if current source files are downloaded',
        CASE WHEN DATE_DIFF('month', MAX(trip_month), CURRENT_DATE) <= 6 THEN 'PASS' ELSE 'WARN' END,
        'Every pipeline run'
    FROM base
)
SELECT
    metric_name,
    metric_definition,
    current_value,
    expected_threshold,
    status,
    update_cadence,
    CURRENT_TIMESTAMP AS calculated_at
FROM metrics;
