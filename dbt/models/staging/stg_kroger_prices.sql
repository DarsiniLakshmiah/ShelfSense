-- stg_kroger_prices: typed + cleaned view over RAW.KROGER_PRICES_RAW
-- One row per product × store × load_date

WITH raw AS (
    SELECT
        load_id,
        loaded_at,
        store_id,
        product_id,
        raw_payload
    FROM {{ source('raw', 'kroger_prices_raw') }}
),

store_lookup AS (
    SELECT
        store_id,
        store_name,
        store_zip,
        store_state,
        store_city
    FROM {{ source('raw', 'kroger_store_lookup') }}
),

parsed AS (
    SELECT
        r.load_id,
        r.loaded_at::DATE                                              AS price_date,
        r.store_id,
        sl.store_name,
        sl.store_zip,
        sl.store_state,
        r.raw_payload:productId::VARCHAR                               AS item_id,
        r.raw_payload:description::VARCHAR                             AS item_name,
        r.raw_payload:upc::VARCHAR                                     AS upc,
        r.raw_payload:categories[0]::VARCHAR                           AS category,
        r.raw_payload:items[0]:price:regular::NUMBER(10,2)             AS regular_price,
        r.raw_payload:items[0]:price:promo::NUMBER(10,2)               AS promo_price,
        IFF(r.raw_payload:items[0]:price:promo IS NOT NULL, TRUE, FALSE) AS is_on_sale,
        r.raw_payload:items[0]:size::VARCHAR                           AS item_size,
        r.raw_payload:items[0]:soldBy::VARCHAR                         AS sold_by
    FROM raw r
    LEFT JOIN store_lookup sl ON r.store_id = sl.store_id
    WHERE r.raw_payload IS NOT NULL
)

SELECT *
FROM parsed
WHERE item_id IS NOT NULL
  AND regular_price IS NOT NULL
  AND regular_price >= 0
