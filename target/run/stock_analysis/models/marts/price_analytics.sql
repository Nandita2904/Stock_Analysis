
  
    

  create  table "stock_analysis"."public"."price_analytics__dbt_tmp"
  
  
    as
  
  (
    

WITH moving_averages AS (
    SELECT
        symbol,
        trade_date,
        AVG(avg_price) OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as ma_5_day,
        AVG(avg_price) OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as ma_10_day,
        AVG(avg_price) OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as ma_20_day
    FROM "stock_analysis"."public"."int_stock_metrics"
),

price_momentum AS (
    SELECT
        symbol,
        trade_date,
        (close_price - LAG(close_price, 5) OVER (PARTITION BY symbol ORDER BY trade_date)) / 
        LAG(close_price, 5) OVER (PARTITION BY symbol ORDER BY trade_date) * 100 as momentum_5_day
    FROM "stock_analysis"."public"."int_stock_metrics"
)

SELECT
    m.symbol,
    m.trade_date,
    s.avg_price,
    s.open_price,
    s.close_price,
    s.high_price,
    s.low_price,
    s.daily_volume,
    s.daily_volatility,
    s.daily_return,
    m.ma_5_day,
    m.ma_10_day,
    m.ma_20_day,
    p.momentum_5_day,
    CASE
        WHEN m.ma_5_day > m.ma_20_day THEN 'Bullish'
        WHEN m.ma_5_day < m.ma_20_day THEN 'Bearish'
        ELSE 'Neutral'
    END as trend_signal
FROM "stock_analysis"."public"."int_stock_metrics" s
JOIN moving_averages m ON s.symbol = m.symbol AND s.trade_date = m.trade_date
LEFT JOIN price_momentum p ON s.symbol = p.symbol AND s.trade_date = p.trade_date
  );
  