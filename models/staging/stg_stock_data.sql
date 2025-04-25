{{ config(materialized='view') }}

SELECT
    id,
    symbol,
    price,
    volume,
    change_percent,
    timestamp,
    sector,
    processed_timestamp
FROM {{ source('stock_data', 'raw_stock_data') }}
