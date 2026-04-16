-- Test: no negative prices in mart_price_history
-- dbt test convention: query returns rows = failure

SELECT
    item_id,
    store_id,
    price_date,
    regular_price
FROM {{ ref('mart_price_history') }}
WHERE regular_price < 0
   OR promo_price < 0
