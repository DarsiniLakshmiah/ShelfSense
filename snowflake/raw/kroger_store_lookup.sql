-- ============================================================
-- RAW.KROGER_STORE_LOOKUP — static store metadata
-- Populated by the daily DAG when discovering store locations.
-- One row per locationId; updated (MERGE) on each discovery.
-- ============================================================

USE DATABASE SHELFSENSE;
USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS KROGER_STORE_LOOKUP (
    store_id        VARCHAR(32)     NOT NULL PRIMARY KEY,
    store_name      VARCHAR(256),
    store_zip       VARCHAR(10),
    store_state     VARCHAR(2),
    store_city      VARCHAR(128),
    _updated_at     TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE KROGER_STORE_LOOKUP IS 'Kroger store metadata (name, zip, state) keyed by locationId';
