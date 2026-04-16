-- ============================================================
-- MARTS.MART_EXTERNAL_SIGNALS
-- ============================================================

USE DATABASE SHELFSENSE;
USE SCHEMA MARTS;

CREATE TABLE IF NOT EXISTS MART_EXTERNAL_SIGNALS (
    signal_date           DATE           NOT NULL,
    signal_type           VARCHAR(32)    NOT NULL,  -- USDA | BLS | NOAA
    commodity             VARCHAR(128),
    value                 NUMBER(14,4),
    yoy_change_pct        NUMBER(10,4),
    weeks_to_retail_impact INTEGER,
    _loaded_at            TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE MART_EXTERNAL_SIGNALS IS 'Cleaned external economic/weather signals joined by date';
