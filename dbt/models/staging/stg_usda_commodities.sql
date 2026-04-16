-- stg_usda_commodities: typed USDA commodity price records
-- Source: RAW.EXTERNAL_SIGNALS_RAW where source = 'USDA'

WITH raw AS (
    SELECT
        load_id,
        loaded_at,
        raw_payload
    FROM {{ source('raw', 'external_signals_raw') }}
    WHERE source = 'USDA'
),

parsed AS (
    SELECT
        load_id,
        loaded_at,
        raw_payload:commodity_desc::VARCHAR                       AS commodity,
        raw_payload:statisticcat_desc::VARCHAR                    AS statistic_category,
        raw_payload:unit_desc::VARCHAR                            AS unit,
        raw_payload:year::INTEGER                                 AS year,
        raw_payload:week_ending::DATE                             AS week_ending_date,
        TRY_TO_NUMBER(raw_payload:Value::VARCHAR, 10, 4)          AS price_value,
        raw_payload:state_name::VARCHAR                           AS state_name,
        raw_payload:agg_level_desc::VARCHAR                       AS agg_level
    FROM raw
    WHERE raw_payload IS NOT NULL
)

SELECT *
FROM parsed
WHERE price_value IS NOT NULL
  AND week_ending_date IS NOT NULL
