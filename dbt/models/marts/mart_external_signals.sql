-- mart_external_signals: unified external economic and weather signals
-- Sources: stg_usda_commodities, stg_noaa_weather
-- Populated weekly by the weekly_signals_dag after USDA Tuesday release.

WITH usda AS (
    SELECT
        week_ending_date                          AS signal_date,
        'USDA'                                    AS signal_type,
        commodity                                 AS commodity,
        price_value                               AS value,
        -- Year-over-year % change: compare to same commodity 52 weeks prior
        ROUND(
            100.0 * (
                price_value
                - LAG(price_value, 52) OVER (
                    PARTITION BY commodity, state_name
                    ORDER BY week_ending_date
                )
            ) / NULLIF(
                LAG(price_value, 52) OVER (
                    PARTITION BY commodity, state_name
                    ORDER BY week_ending_date
                ),
                0
            ),
            4
        )                                         AS yoy_change_pct,
        -- Estimated retail lag (weeks) from IV analysis — hardcoded defaults
        CASE commodity
            WHEN 'EGGS'     THEN 2
            WHEN 'BROILERS' THEN 2
            WHEN 'CATTLE'   THEN 3
            WHEN 'MILK'     THEN 2
            ELSE 2
        END                                       AS weeks_to_retail_impact
    FROM {{ ref('stg_usda_commodities') }}
),

noaa AS (
    SELECT
        observation_date                          AS signal_date,
        'NOAA'                                    AS signal_type,
        -- Classify the weather event type as the commodity label
        CASE
            WHEN is_frost_event THEN 'FROST_EVENT'
            WHEN is_heat_event  THEN 'HEAT_EVENT'
            ELSE 'WEATHER_OBSERVATION'
        END                                       AS commodity,
        -- Use max temp (in tenths of °C) as the numeric value
        tmax_tenths_c / 10.0                      AS value,
        NULL::NUMBER(10,4)                        AS yoy_change_pct,
        1                                         AS weeks_to_retail_impact
    FROM {{ ref('stg_noaa_weather') }}
    WHERE is_frost_event OR is_heat_event
),

bls AS (
    SELECT
        series_date                                           AS signal_date,
        'BLS'                                                 AS signal_type,
        series_id                                             AS commodity,
        value                                                 AS value,
        ROUND(
            100.0 * (
                value
                - LAG(value, 12) OVER (PARTITION BY series_id ORDER BY series_date)
            ) / NULLIF(
                LAG(value, 12) OVER (PARTITION BY series_id ORDER BY series_date),
                0
            ),
            4
        )                                                     AS yoy_change_pct,
        2                                                     AS weeks_to_retail_impact
    FROM {{ ref('stg_bls_series') }}
),

combined AS (
    SELECT * FROM usda
    UNION ALL
    SELECT * FROM noaa
    UNION ALL
    SELECT * FROM bls
)

SELECT
    signal_date,
    signal_type,
    commodity,
    value,
    yoy_change_pct,
    weeks_to_retail_impact,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM combined
WHERE signal_date IS NOT NULL
  AND value IS NOT NULL
ORDER BY signal_date DESC
