-- Analyze batch processing metrics
WITH batch_run_times AS (
    SELECT
        DATE(created_at) as run_date,
        model_name,
        EXTRACT(EPOCH FROM (completed_at - started_at)) as execution_time_seconds
    FROM dbt_runs
),

model_execution_stats AS (
    SELECT
        run_date,
        model_name,
        AVG(execution_time_seconds) as avg_execution_time,
        MAX(execution_time_seconds) as max_execution_time,
        MIN(execution_time_seconds) as min_execution_time
    FROM batch_run_times
    GROUP BY run_date, model_name
)

SELECT
    run_date,
    model_name,
    avg_execution_time,
    max_execution_time,
    min_execution_time
FROM model_execution_stats
ORDER BY run_date, avg_execution_time DESC;

-- Analyze data volume processed
SELECT
    DATE(trade_date) as analysis_date,
    COUNT(*) as total_records,
    COUNT(DISTINCT symbol) as total_symbols,
    SUM(daily_volume) as total_trade_volume
FROM int_stock_metrics
GROUP BY DATE(trade_date)
ORDER BY analysis_date;
