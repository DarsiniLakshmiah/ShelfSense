-- mart_promo_calendar: promo cycle history per item × store
-- Detects sale start/end dates and computes avg cycle length

WITH prices AS (
    SELECT * FROM {{ ref('mart_price_history') }}
),

-- Identify when a sale starts (is_on_sale transitions from FALSE→TRUE)
sale_events AS (
    SELECT
        item_id,
        store_id,
        price_date,
        is_on_sale,
        regular_price,
        promo_price,
        LAG(is_on_sale) OVER (
            PARTITION BY item_id, store_id ORDER BY price_date
        ) AS prev_on_sale
    FROM prices
),

sale_starts AS (
    SELECT
        item_id,
        store_id,
        price_date AS sale_start_date,
        promo_price,
        regular_price,
        ROUND(1.0 - promo_price / NULLIF(regular_price, 0), 4) AS sale_depth_pct
    FROM sale_events
    WHERE is_on_sale = TRUE
      AND (prev_on_sale = FALSE OR prev_on_sale IS NULL)
),

sale_ends AS (
    SELECT
        item_id,
        store_id,
        -- Sale ends on the first day after the promo window
        LEAD(price_date) OVER (
            PARTITION BY item_id, store_id ORDER BY price_date
        ) AS sale_end_date,
        price_date AS last_promo_date
    FROM sale_events
    WHERE is_on_sale = FALSE
      AND prev_on_sale = TRUE
),

-- Join starts with approximate ends
combined AS (
    SELECT
        ss.item_id,
        ss.store_id,
        ss.sale_start_date,
        se.sale_end_date,
        ss.sale_depth_pct,
        DATEDIFF('day',
            LAG(ss.sale_start_date) OVER (PARTITION BY ss.item_id, ss.store_id ORDER BY ss.sale_start_date),
            ss.sale_start_date
        ) AS days_since_last_sale
    FROM sale_starts ss
    LEFT JOIN sale_ends se
        ON ss.item_id = se.item_id
        AND ss.store_id = se.store_id
        AND se.last_promo_date >= ss.sale_start_date
),

with_avg AS (
    SELECT
        *,
        ROUND(AVG(days_since_last_sale) OVER (
            PARTITION BY item_id, store_id
        ), 2) AS avg_days_between_sales
    FROM combined
)

SELECT
    item_id,
    store_id,
    sale_start_date,
    sale_end_date,
    sale_depth_pct,
    days_since_last_sale,
    avg_days_between_sales,
    -- predicted_next_sale_date populated by ML classifier
    NULL::DATE AS predicted_next_sale_date
FROM with_avg
ORDER BY item_id, store_id, sale_start_date
