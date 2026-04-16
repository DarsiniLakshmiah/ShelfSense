-- ============================================================
-- RAW.EXTERNAL_SIGNALS_RAW — USDA / BLS / NOAA / OpenPrices
-- ============================================================

USE DATABASE SHELFSENSE;
USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS EXTERNAL_SIGNALS_RAW (
    load_id      VARCHAR(64)    NOT NULL DEFAULT UUID_STRING(),
    loaded_at    TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    source       VARCHAR(32)    NOT NULL,  -- 'USDA' | 'BLS' | 'NOAA' | 'OPEN_PRICES'
    raw_payload  VARIANT        NOT NULL
);

ALTER TABLE EXTERNAL_SIGNALS_RAW CLUSTER BY (source, DATE_TRUNC('day', loaded_at));

COMMENT ON TABLE EXTERNAL_SIGNALS_RAW IS 'Raw JSON from USDA NASS, BLS, NOAA CDO, and Open Prices APIs';
