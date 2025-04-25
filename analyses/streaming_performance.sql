-- Analysis of streaming calculation accuracy
WITH streaming_avg AS (
    SELECT
        symbol,
        window_size,
        avg_price,
        DATE(window_end_time) as analysis_date
    FROM stock_moving_averages
    WHERE window_size = '30min'
),

batch_avg AS (
    SELECT
        symbol,
        AVG(avg_price) as batch_avg_price,
        trade_date as analysis_date
    FROM int_stock_metrics
    GROUP BY symbol, trade_date
)

SELECT
    s.symbol,
    s.analysis_date,
    s.avg_price as streaming_avg_price,
    b.batch_avg_price,
    (s.avg_price - b.batch_avg_price) as price_difference,
    ((s.avg_price - b.batch_avg_price) / b.batch_avg_price) * 100 as percentage_difference
FROM streaming_avg s
JOIN batch_avg b ON s.symbol = b.symbol AND s.analysis_date = b.analysis_date
ORDER BY percentage_difference DESC;

-- Streaming processing statistics
SELECT
    DATE(window_end_time) as analysis_date,
    COUNT(*) as total_records,
    AVG(EXTRACT(EPOCH FROM (processed_timestamp - window_end_time))) as avg_processing_delay_seconds
FROM stock_moving_averages
GROUP BY DATE(window_end_time)
ORDER BY analysis_date;
