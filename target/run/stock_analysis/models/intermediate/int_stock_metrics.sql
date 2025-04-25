
  
    

  create  table "stock_analysis"."public"."int_stock_metrics__dbt_tmp"
  
  
    as
  
  (
    

WITH daily_metrics AS (
    SELECT
        symbol,
        DATE(timestamp) as trade_date,
        AVG(price) as avg_price,
        MAX(price) as high_price,
        MIN(price) as low_price,
        -- Remove FIRST_VALUE and LAST_VALUE here, instead calculate in a separate CTE
        SUM(volume) as daily_volume
    FROM "stock_analysis"."public"."stg_stock_data"
    GROUP BY symbol, DATE(timestamp)
),

open_close_prices AS (
    SELECT
        symbol,
        DATE(timestamp) as trade_date,
        FIRST_VALUE(price) OVER (PARTITION BY symbol, DATE(timestamp) ORDER BY timestamp) as open_price,
        LAST_VALUE(price) OVER (PARTITION BY symbol, DATE(timestamp) ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as close_price
    FROM "stock_analysis"."public"."stg_stock_data"
),

volatility AS (
    SELECT
        dm.symbol,
        dm.trade_date,
        (dm.high_price - dm.low_price) / ((dm.high_price + dm.low_price) / 2) * 100 as daily_volatility
    FROM daily_metrics dm
)

SELECT
    dm.symbol,
    dm.trade_date,
    dm.avg_price,
    dm.high_price,
    dm.low_price,
    op.open_price,
    op.close_price,
    dm.daily_volume,
    vol.daily_volatility,
    (op.close_price - op.open_price) / op.open_price * 100 as daily_return
FROM daily_metrics dm
JOIN open_close_prices op ON dm.symbol = op.symbol AND dm.trade_date = op.trade_date
JOIN volatility vol ON dm.symbol = vol.symbol AND dm.trade_date = vol.trade_date
  );
  