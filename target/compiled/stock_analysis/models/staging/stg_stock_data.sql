

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