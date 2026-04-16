-- ============================================================
-- MARTS.MART_PROMO_CALENDAR
-- ============================================================

USE DATABASE SHELFSENSE;
USE SCHEMA MARTS;

CREATE TABLE IF NOT EXISTS MART_PROMO_CALENDAR (
    item_id                  VARCHAR(64)    NOT NULL,
    store_id                 VARCHAR(32)    NOT NULL,
    sale_start_date          DATE,
    sale_end_date            DATE,
    sale_depth_pct           NUMBER(6,4),
    days_since_last_sale     INTEGER,
    avg_days_between_sales   NUMBER(8,2),
    predicted_next_sale_date DATE,
    _loaded_at               TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE MART_PROMO_CALENDAR IS 'Promo cycle history and ML-predicted next sale dates';
