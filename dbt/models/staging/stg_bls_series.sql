-- stg_bls_series: typed BLS price/CPI series records
-- Source: RAW.EXTERNAL_SIGNALS_RAW where source = 'BLS'

WITH raw AS (
    SELECT
        load_id,
        loaded_at,
        raw_payload
    FROM {{ source('raw', 'external_signals_raw') }}
    WHERE source = 'BLS'
),

parsed AS (
    SELECT
        load_id,
        loaded_at,
        raw_payload:series_id::VARCHAR                            AS series_id,
        raw_payload:year::INTEGER                                 AS year,
        raw_payload:period::VARCHAR                               AS period,      -- e.g. "M01"
        raw_payload:period_name::VARCHAR                          AS period_name, -- e.g. "January"
        TRY_TO_NUMBER(raw_payload:value::VARCHAR, 10, 4)          AS value,
        -- Derive an approximate date from year + period (monthly series)
        TRY_TO_DATE(
            raw_payload:year::VARCHAR || '-' ||
            LPAD(REPLACE(raw_payload:period::VARCHAR, 'M', ''), 2, '0') || '-01',
            'YYYY-MM-DD'
        )                                                         AS series_date
    FROM raw
    WHERE raw_payload IS NOT NULL
      AND raw_payload:period::VARCHAR LIKE 'M%'  -- monthly periods only
)

SELECT *
FROM parsed
WHERE value IS NOT NULL
  AND series_date IS NOT NULL
