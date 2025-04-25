
  create view "stock_analysis"."public"."stg_stock_data__dbt_tmp"
    
    
  as (
    

SELECT
    id,
    symbol,
    price,
    volume,
    change_percent,
    timestamp,
    sector,
    processed_timestamp
FROM "stock_analysis"."public"."raw_stock_data"
  );