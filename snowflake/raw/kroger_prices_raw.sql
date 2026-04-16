-- ============================================================
-- RAW.KROGER_PRICES_RAW — landing table for Kroger API payloads
-- ============================================================

USE DATABASE SHELFSENSE;
USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS KROGER_PRICES_RAW (
    load_id         VARCHAR(64)     NOT NULL DEFAULT UUID_STRING(),
    loaded_at       TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    store_id        VARCHAR(32),
    product_id      VARCHAR(64),
    raw_payload     VARIANT         NOT NULL   -- full JSON from Kroger API
);

-- Clustered on date for efficient time-range scans
ALTER TABLE KROGER_PRICES_RAW CLUSTER BY (DATE_TRUNC('day', loaded_at));

COMMENT ON TABLE KROGER_PRICES_RAW IS 'Raw JSON payloads from Kroger Products API, one row per product × store × load';
