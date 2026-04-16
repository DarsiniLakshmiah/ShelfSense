-- stg_noaa_weather: typed NOAA daily weather observations
-- Source: RAW.EXTERNAL_SIGNALS_RAW where source = 'NOAA'

WITH raw AS (
    SELECT
        load_id,
        loaded_at,
        raw_payload
    FROM {{ source('raw', 'external_signals_raw') }}
    WHERE source = 'NOAA'
),

parsed AS (
    SELECT
        load_id,
        loaded_at,
        raw_payload:date::DATE                                    AS observation_date,
        raw_payload:station::VARCHAR                              AS station_id,
        raw_payload:datatype::VARCHAR                             AS data_type,
        TRY_TO_NUMBER(raw_payload:value::VARCHAR, 10, 2)          AS observation_value,
        raw_payload:attributes::VARCHAR                           AS attributes
    FROM raw
    WHERE raw_payload IS NOT NULL
),

-- Pivot wide so each row is one station × date with all metrics
pivoted AS (
    SELECT
        observation_date,
        station_id,
        MAX(CASE WHEN data_type = 'TMAX' THEN observation_value END) AS tmax_tenths_c,
        MAX(CASE WHEN data_type = 'TMIN' THEN observation_value END) AS tmin_tenths_c,
        MAX(CASE WHEN data_type = 'PRCP' THEN observation_value END) AS prcp_tenths_mm,
        MAX(CASE WHEN data_type = 'SNOW' THEN observation_value END) AS snow_mm,
        COUNT(DISTINCT load_id)                                        AS source_loads
    FROM parsed
    GROUP BY 1, 2
),

final AS (
    SELECT
        *,
        -- Frost flag: TMIN below 0°C (0 in tenths = 0°C)
        IFF(tmin_tenths_c IS NOT NULL AND tmin_tenths_c <= 0, TRUE, FALSE) AS is_frost_event,
        -- Heat flag: TMAX above 38°C (380 in tenths)
        IFF(tmax_tenths_c IS NOT NULL AND tmax_tenths_c >= 380, TRUE, FALSE) AS is_heat_event
    FROM pivoted
)

SELECT *
FROM final
