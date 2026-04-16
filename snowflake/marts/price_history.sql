-- ============================================================
-- MARTS.MART_PRICE_HISTORY — core analytical price table
-- Populated by dbt; this DDL is reference / initial scaffold
-- ============================================================

USE DATABASE SHELFSENSE;
USE SCHEMA MARTS;

CREATE TABLE IF NOT EXISTS MART_PRICE_HISTORY (
    item_id              VARCHAR(64)    NOT NULL,
    item_name            VARCHAR(256),
    upc                  VARCHAR(32),
    category             VARCHAR(128),
    store_id             VARCHAR(32)    NOT NULL,
    store_name           VARCHAR(256),
    store_zip            VARCHAR(10),
    store_state          VARCHAR(2),
    price_date           DATE           NOT NULL,
    regular_price        NUMBER(10,2),
    promo_price          NUMBER(10,2),
    is_on_sale           BOOLEAN,
    price_delta_7d       NUMBER(10,4),
    price_delta_30d      NUMBER(10,4),
    price_vs_90d_avg     NUMBER(10,4),
    _loaded_at           TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE MART_PRICE_HISTORY IS 'Business-ready price history with rolling deltas';
