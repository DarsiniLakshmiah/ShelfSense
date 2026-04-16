-- mart_price_history: core analytical price table
-- Adds rolling deltas and store metadata

WITH prices AS (
    SELECT * FROM {{ ref('stg_kroger_prices') }}
),

-- Rolling aggregates using window functions
with_deltas AS (
    SELECT
        p.item_id,
        p.item_name,
        p.upc,
        p.category,
        p.store_id,
        p.store_name,
        p.store_zip,
        p.store_state,
        p.price_date,
        p.regular_price,
        p.promo_price,
        p.is_on_sale,
        p.item_size,
        -- 7-day delta: compare to price 7 days ago
        p.regular_price - LAG(p.regular_price, 7) OVER (
            PARTITION BY p.item_id, p.store_id
            ORDER BY p.price_date
        ) AS price_delta_7d,
        -- 30-day delta
        p.regular_price - LAG(p.regular_price, 30) OVER (
            PARTITION BY p.item_id, p.store_id
            ORDER BY p.price_date
        ) AS price_delta_30d,
        -- vs 90-day rolling average
        p.regular_price - AVG(p.regular_price) OVER (
            PARTITION BY p.item_id, p.store_id
            ORDER BY p.price_date
            ROWS BETWEEN 90 PRECEDING AND 1 PRECEDING
        ) AS price_vs_90d_avg
    FROM prices p
)

SELECT *
FROM with_deltas
ORDER BY item_id, store_id, price_date
