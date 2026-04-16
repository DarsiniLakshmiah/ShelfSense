-- mart_buy_signals: ML model output — buy now vs. wait recommendations
-- Populated by ml/classifier.py; this DDL creates the empty shell
-- that the Python ML job writes into via Snowflake connector.

{{
    config(
        materialized='table',
        post_hook="ALTER TABLE {{ this }} CLUSTER BY (item_id, store_id, signal_date)"
    )
}}

WITH base AS (
    -- Pull latest price_history as the base for signals
    SELECT
        item_id,
        store_id,
        price_date                  AS signal_date,
        regular_price,
        promo_price,
        is_on_sale,
        price_delta_7d,
        price_delta_30d,
        price_vs_90d_avg
    FROM {{ ref('mart_price_history') }}
    WHERE price_date = (
        SELECT MAX(price_date)
        FROM {{ ref('mart_price_history') }}
    )
)

-- Placeholder structure — ML job overwrites predicted columns
SELECT
    item_id,
    store_id,
    signal_date,
    regular_price,
    NULL::NUMBER(10,2)   AS predicted_price_7d,
    NULL::NUMBER(10,2)   AS predicted_price_14d,
    NULL::NUMBER(6,4)    AS buy_now_probability,
    NULL::VARCHAR(16)    AS recommendation,     -- BUY_NOW | WAIT | NEUTRAL
    NULL::VARCHAR(32)    AS model_version,
    NULL::NUMBER(10,2)   AS confidence_interval_lower,
    NULL::NUMBER(10,2)   AS confidence_interval_upper
FROM base
