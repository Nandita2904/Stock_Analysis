
  
    

  create  table "stock_analysis"."public"."int_sector_analysis__dbt_tmp"
  
  
    as
  
  (
    

WITH sector_daily AS (
    SELECT
        sector,
        DATE(timestamp) as trade_date,
        AVG(price) as sector_avg_price,
        SUM(volume) as sector_volume,
        AVG(change_percent) as sector_avg_change
    FROM "stock_analysis"."public"."stg_stock_data"
    GROUP BY sector, DATE(timestamp)
),

sector_volatility AS (
    SELECT
        s.sector,
        m.trade_date,
        STDDEV(m.daily_volatility) as sector_volatility
    FROM "stock_analysis"."public"."int_stock_metrics" m
    JOIN "stock_analysis"."public"."stg_stock_data" s ON m.symbol = s.symbol
    GROUP BY s.sector, m.trade_date
)

SELECT
    sd.sector,
    sd.trade_date,
    sd.sector_avg_price,
    sd.sector_volume,
    sd.sector_avg_change,
    sv.sector_volatility,
    RANK() OVER (PARTITION BY sd.trade_date ORDER BY sd.sector_avg_change DESC) as performance_rank
FROM sector_daily sd
LEFT JOIN sector_volatility sv ON sd.sector = sv.sector AND sd.trade_date = sv.trade_date
  );
  