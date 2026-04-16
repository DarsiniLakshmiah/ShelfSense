-- ============================================================
-- MARTS.MART_BUY_SIGNALS — ML classifier output
-- Populated by ml/classifier.py via the ml_buy_signals DAG task
-- ============================================================

USE DATABASE SHELFSENSE;
USE SCHEMA MARTS;

CREATE TABLE IF NOT EXISTS MART_BUY_SIGNALS (
    item_id                     VARCHAR(64)    NOT NULL,
    store_id                    VARCHAR(32)    NOT NULL,
    signal_date                 DATE           NOT NULL,
    regular_price               NUMBER(10,2),
    predicted_price_7d          NUMBER(10,2),
    predicted_price_14d         NUMBER(10,2),
    buy_now_probability         NUMBER(6,4),
    recommendation              VARCHAR(16),   -- BUY_NOW / WAIT / NEUTRAL
    model_version               VARCHAR(32),
    confidence_interval_lower   NUMBER(10,2),
    confidence_interval_upper   NUMBER(10,2),
    _loaded_at                  TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE MART_BUY_SIGNALS IS 'ML buy-now classifier output: per-item per-store daily recommendations';
